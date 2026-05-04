using System.Buffers;
using System.Net.WebSockets;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server;

/// <summary>
/// Per-WebSocket-connection state. Manages subscriptions and outbound message ring.
/// All book/trade events arrive pre-serialized from upstream conflation in SubscriptionManager.
/// The write loop coalesces pre-serialized messages + dirty-flag info into single WebSocket frames.
/// </summary>
public sealed class ClientSession : IDisposable
{
    private static int _nextId;

    public string Id { get; }
    public WebSocket Socket { get; }
    private readonly MpscOutboundRing _outbound;
    // ALL mutations and iterations of _subscriptions go through _subscriptionsLock.
    // Historically this set was treated as feed-thread-owned, but unsubscribe paths
    // (and Subscriptions / IsSubscribed callers) can run on the WS read thread, so
    // every read/write is now serialised here. The set is small and writes are rare;
    // contention is negligible and a single lock makes the invariant local + obvious.
    private readonly object _subscriptionsLock = new();
    private readonly HashSet<ulong> _subscriptions = new();
    // Owned by RunWriteLoopAsync. Populated/cleared exclusively via OutboundKind.AddInfoSub/
    // RemoveInfoSub deltas drained from _outbound, so no synchronization is needed.
    private readonly Dictionary<ulong, long> _infoVersions = new();
    private MarketDataManager[]? _marketDataManagers;
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger _logger;
    private readonly double _slowClientThreshold;
    private readonly int _slowClientMaxTicks;
    private readonly long _maxPendingBytes;
    private readonly int _coalesceWindowMs;

    private long _messagesSent;
    private long _bytesSent;
    private long _pendingBytes;
    private long _broadcastDropCount;
    private int _disconnectRequested;
    private int _infoWakePending;

    /// <summary>
    /// Snapshot of currently subscribed securityIds. Returns a fresh array under
    /// <see cref="_subscriptionsLock"/> so callers can iterate without observing
    /// concurrent mutations.
    /// </summary>
    public IReadOnlyCollection<ulong> Subscriptions
    {
        get
        {
            lock (_subscriptionsLock)
                return _subscriptions.Count == 0 ? Array.Empty<ulong>() : _subscriptions.ToArray();
        }
    }
    public CancellationToken CancellationToken => _cts.Token;

    /// <summary>Total messages (pre-serialized + info) sent to this client.</summary>
    public long MessagesSent => Volatile.Read(ref _messagesSent);

    /// <summary>Total bytes sent to this client.</summary>
    public long BytesSent => Volatile.Read(ref _bytesSent);

    /// <summary>
    /// Bytes currently sitting in the outbound ring (enqueued by producers, not yet
    /// drained by the write loop). Used for the byte-budget slow-consumer guard so
    /// large coalesced payloads cannot accumulate unbounded memory before the
    /// queue-depth threshold trips.
    /// </summary>
    public long PendingBytes => Volatile.Read(ref _pendingBytes);

    /// <summary>
    /// Hard cap (in bytes) of pending payload sitting in the outbound ring. Producers
    /// disconnect the client immediately on enqueue if accepting the new payload would
    /// exceed this budget. 0 disables the check (back-compat for tests).
    /// </summary>
    public long MaxPendingBytes => _maxPendingBytes;

    /// <summary>
    /// Coalescing window (ms) the write loop waits after a wake-up before draining,
    /// to accumulate more messages into a single WebSocket frame. 0 = drain immediately.
    /// </summary>
    public int CoalesceWindowMs => _coalesceWindowMs;

    /// <summary>
    /// Cumulative number of global broadcast payloads (rankings / recovery progress / news)
    /// dropped because this session's outbound ring was full. Incremented by publishers
    /// that intentionally do NOT auto-disconnect on a single drop; the
    /// <see cref="OutlierSweeper"/> can later use the counter to act on chronically slow
    /// clients. Reads/writes via <see cref="Volatile"/> + <see cref="Interlocked"/>.
    /// </summary>
    public long BroadcastDropCount => Volatile.Read(ref _broadcastDropCount);

    /// <summary>
    /// Record a dropped global broadcast for this client. Returns the new total count so
    /// publishers can rate-limit warnings on power-of-two thresholds (1, 8, 64, 512, …)
    /// without an extra read.
    /// </summary>
    internal long RecordBroadcastDrop()
    {
        return Interlocked.Increment(ref _broadcastDropCount);
    }

    public ClientSession(
        WebSocket socket,
        int channelCapacity = 4096,
        double slowClientThreshold = 0.75,
        int slowClientMaxTicks = 100,
        long maxPendingBytes = 16L * 1024 * 1024,
        int coalesceWindowMs = 0,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(socket);
        ArgumentOutOfRangeException.ThrowIfLessThan(channelCapacity, 1);
        if (slowClientThreshold is <= 0 or > 1)
            throw new ArgumentOutOfRangeException(nameof(slowClientThreshold), "Threshold must be in the (0, 1] range.");
        ArgumentOutOfRangeException.ThrowIfLessThan(slowClientMaxTicks, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(maxPendingBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(coalesceWindowMs);

        Id = $"client-{Interlocked.Increment(ref _nextId)}";
        Socket = socket;
        ChannelCapacity = channelCapacity;
        _slowClientThreshold = slowClientThreshold;
        _slowClientMaxTicks = slowClientMaxTicks;
        _maxPendingBytes = maxPendingBytes;
        _coalesceWindowMs = coalesceWindowMs;
        _logger = logger ?? NullLogger.Instance;
        // Lock-free MPSC ring rounds capacity up to the next power of two; the
        // user-facing ChannelCapacity stays as the requested value (used as the
        // slow-client threshold denominator and for back-compat with tests).
        _outbound = new MpscOutboundRing(channelCapacity);
    }

    public bool IsSubscribed(ulong securityId)
    {
        lock (_subscriptionsLock)
            return _subscriptions.Contains(securityId);
    }

    private int _newsSubscriptionCount;

    /// <summary>Fast lock-free check used by the global-news fan-out path to skip
    /// clients with zero News-flag subscriptions. Tracked by ref-counting in
    /// <see cref="SubscriptionManager"/> on every subscribe/unsubscribe transition
    /// where the <see cref="DataFlags.News"/> bit changes for this session.</summary>
    public bool HasAnyNewsSubscription => Volatile.Read(ref _newsSubscriptionCount) > 0;

    internal void IncrementNewsSubscriptions() => Interlocked.Increment(ref _newsSubscriptionCount);

    internal void DecrementNewsSubscriptions()
    {
        // Defensive: never let the counter go negative; one missed delta would otherwise
        // permanently disable global-news fan-out for this session.
        int updated = Interlocked.Decrement(ref _newsSubscriptionCount);
        if (updated < 0) Interlocked.Exchange(ref _newsSubscriptionCount, 0);
    }

    /// <summary>Add a subscription. May be called from any thread; serialised through
    /// <see cref="_subscriptionsLock"/>.</summary>
    public void AddSubscription(ulong securityId)
    {
        lock (_subscriptionsLock)
            _subscriptions.Add(securityId);
    }

    /// <summary>Remove a subscription. May be called from the feed thread or WS read
    /// thread; the HashSet mutation is guarded by <see cref="_subscriptionsLock"/>.
    /// The info-version delete is routed through the outbound ring so the WriteLoop
    /// is the sole writer to <c>_infoVersions</c>.</summary>
    public bool RemoveSubscription(ulong securityId)
    {
        lock (_subscriptionsLock)
            _subscriptions.Remove(securityId);
        return RemoveInfoSubscription(securityId);
    }

    /// <summary>Track a security for dirty-flag info delivery. Called on the feed thread.
    /// Routes through the outbound ring so <c>_infoVersions</c> remains a plain
    /// (lock-free) <see cref="Dictionary{TKey, TValue}"/> mutated only by the WriteLoop.</summary>
    public bool AddInfoSubscription(ulong securityId) =>
        TryEnqueueCore(OutboundMessage.AddInfoSub(securityId), "add-info-sub");

    public bool RemoveInfoSubscription(ulong securityId) =>
        TryEnqueueCore(OutboundMessage.RemoveInfoSub(securityId), "remove-info-sub");

    /// <summary>Set the MarketDataManagers for on-demand info reads (one per group).</summary>
    public void SetMarketDataManagers(MarketDataManager[] managers) =>
        _marketDataManagers = managers;

    /// <summary>Current outbound queue depth (approximate, MPSC ring observation).</summary>
    public int QueueDepth => _outbound.ApproximateDepth;

    /// <summary>Soft capacity reference for slow-client detection.</summary>
    public int ChannelCapacity { get; }

    // --- Enqueue methods (called from feed thread / SubscriptionManager) ---

    /// <summary>
    /// Enqueue a pre-serialized message. All events (orders, trades, clears, control)
    /// arrive pre-serialized from upstream conflation. If the queue is full,
    /// the client is disconnected to preserve feed health.
    /// </summary>
    public bool TryEnqueue(ReadOnlyMemory<byte> message) =>
        TryEnqueueCore(new OutboundMessage(message), "payload");

    /// <summary>
    /// Enqueue a coalesced batch of <paramref name="logicalMessageCount"/> pre-serialized
    /// messages packed back-to-back in <paramref name="batch"/>. Used by the per-group
    /// flush path in <see cref="GroupConflationHandler"/> to amortize the per-event
    /// Channel.TryWrite cost (one Monitor acquisition under contention) across an
    /// entire flush cycle. The write loop concatenates messages identically to the
    /// per-message path; only the queue-write count is reduced.
    ///
    /// When <paramref name="pooledArray"/> is provided, ownership transfers to the
    /// session: the write loop returns it to <see cref="BroadcastBufferPool.Shared"/>
    /// after the WS frame is sent. If the channel is full and the client is
    /// disconnected, the array is returned synchronously here. Empty/zero-count
    /// batches are no-ops and the array (if any) is returned immediately.
    /// The pooled array MUST originate from <see cref="BroadcastBufferPool.Shared"/>;
    /// arrays from elsewhere will be silently dropped to the GC on return.
    /// </summary>
    public bool TryEnqueueBatch(ReadOnlyMemory<byte> batch, int logicalMessageCount, byte[]? pooledArray = null)
    {
        if (batch.IsEmpty || logicalMessageCount <= 0)
        {
            if (pooledArray is not null) BroadcastBufferPool.Shared.Return(pooledArray);
            return true;
        }
        return TryEnqueueCore(new OutboundMessage(batch, logicalCount: logicalMessageCount, pooledArray: pooledArray), "batch");
    }

    /// <summary>
    /// Wake the writer so Info subscriptions can flush the latest InstrumentInfo version.
    /// Coalesces multiple updates into at most one pending wake item per client.
    /// </summary>
    public bool NotifyInfoAvailable()
    {
        if (Interlocked.Exchange(ref _infoWakePending, 1) != 0)
            return true;

        return TryEnqueueCore(OutboundMessage.InfoWake, "info update");
    }

    // --- Write loop: coalesce + dirty-flag info ---

    private const int MaxDrainPerCycle = 16384;

    /// <summary>
    /// Write loop: drains the outbound MPSC ring (pre-serialized messages),
    /// reads dirty info via version counters, coalesces everything
    /// into a single WebSocket binary frame.
    /// </summary>
    public async Task RunWriteLoopAsync()
    {
        var ct = _cts.Token;
        var coalesceBuf = new byte[65536];
        var messages = new List<ReadOnlyMemory<byte>>();
        var pooledToReturn = new List<byte[]>();
        int slowTicks = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (Socket.State != WebSocketState.Open) break;

                // Phase 1: drain outbound ring into temporary buffers.
                var drained = DrainOutboundQueue(messages, pooledToReturn);
                if (drained.Count == 0)
                {
                    if (await ParkUntilProducerAsync(ct).ConfigureAwait(false))
                        continue;
                    break;
                }
                if (_maxPendingBytes > 0 && drained.PayloadBytes > 0)
                    Interlocked.Add(ref _pendingBytes, -drained.PayloadBytes);
                if (drained.SawInfoWake)
                    Interlocked.Exchange(ref _infoWakePending, 0);

                // Phase 2: coalesce drained payloads into the contiguous send buffer.
                int offset = CoalesceMessages(messages, ref coalesceBuf);

                // Phase 3: append info snapshots for any subscribed securities whose
                // version advanced since we last emitted them (dirty-flag pull model).
                int infoMessages = AppendInfoSnapshots(ref coalesceBuf, ref offset);

                // Phase 4: send + accounting.
                if (offset > 0)
                {
                    // Pass CancellationToken.None: ManagedWebSocket.SendFrameAsync routes
                    // any cancelable token through SendFrameFallbackAsync, which boxes an
                    // async state machine and allocates a CancellationTokenRegistration per
                    // frame. The non-cancelable fast path (SendFrameLockAcquiredNonCancelableAsync)
                    // returns ValueTask.CompletedTask synchronously when the underlying stream
                    // write+flush complete inline. We observe shutdown via the loop-top check on
                    // ct.IsCancellationRequested and on ParkUntilProducerAsync(ct); a coalesced
                    // batch is bounded in size and completes in milliseconds, so letting an
                    // in-flight send finish is preferable to a torn frame.
                    await Socket.SendAsync(coalesceBuf.AsMemory(0, offset),
                        WebSocketMessageType.Binary, true, CancellationToken.None);
                    int totalMessages = drained.LogicalCount + infoMessages;
                    Interlocked.Add(ref _messagesSent, totalMessages);
                    Interlocked.Add(ref _bytesSent, offset);
                    MetricsRegistry.WsMessagesSent.Add(totalMessages);
                }

                // Return pooled producer buffers AFTER SendAsync completes (Payload spans
                // were copied into coalesceBuf, but returning earlier would invalidate the
                // backing arrays held in `messages` if SendAsync inspected them later).
                ReturnPooledBuffers(pooledToReturn);

                // Phase 5: slow-client self-detection.
                if (CheckSlowClient(ref slowTicks))
                    break;
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException ex)
        {
            _logger.LogWarning("Write error on {ClientId}: {Error}", Id, ex.Message);
        }
        finally
        {
            // Ensure any items left in the channel (after Cancel/disconnect) release
            // their pooled buffers so the ArrayPool doesn't accumulate orphans.
            ReturnPooledBuffers(pooledToReturn);
            DrainAndReturnPooled();
        }
    }

    private static void ReturnPooledBuffers(List<byte[]> pooledToReturn)
    {
        for (int i = 0; i < pooledToReturn.Count; i++)
            BroadcastBufferPool.Shared.Return(pooledToReturn[i]);
        pooledToReturn.Clear();
    }

    private readonly record struct DrainResult(
        int Count, int LogicalCount, long PayloadBytes, bool SawInfoWake);

    /// <summary>
    /// Drain up to <see cref="MaxDrainPerCycle"/> entries from the outbound ring,
    /// dispatching each by <see cref="OutboundKind"/>. Payload entries land in
    /// <paramref name="messages"/> (and pooled-array references in <paramref name="pooledToReturn"/>);
    /// info-version deltas mutate <c>_infoVersions</c> in-place; an InfoWake records
    /// that the producer's pending flag should be cleared.
    /// </summary>
    private DrainResult DrainOutboundQueue(
        List<ReadOnlyMemory<byte>> messages, List<byte[]> pooledToReturn)
    {
        messages.Clear();
        pooledToReturn.Clear();
        int drained = 0;
        int logicalDrained = 0;
        long payloadBytesDrained = 0;
        bool sawInfoWake = false;
        while (drained < MaxDrainPerCycle && _outbound.TryDequeue(out var outbound))
        {
            drained++;
            switch (outbound.Kind)
            {
                case OutboundKind.Payload:
                    messages.Add(outbound.Payload);
                    logicalDrained += outbound.LogicalCount;
                    payloadBytesDrained += outbound.Payload.Length;
                    if (outbound.PooledArray is { } pooled)
                        pooledToReturn.Add(pooled);
                    break;
                case OutboundKind.InfoWake:
                    sawInfoWake = true;
                    break;
                case OutboundKind.AddInfoSub:
                    // First time we see this securityId: seed version 0 so the next
                    // info-snapshot scan emits the current value. Re-adds are no-ops.
                    _infoVersions.TryAdd(outbound.SecurityId, 0);
                    break;
                case OutboundKind.RemoveInfoSub:
                    _infoVersions.Remove(outbound.SecurityId);
                    break;
            }
        }
        return new DrainResult(drained, logicalDrained, payloadBytesDrained, sawInfoWake);
    }

    /// <summary>
    /// Park until a producer enqueues. The ring's WaitForItemsAsync re-checks after
    /// publishing the waiting flag, so producers that raced our last failed TryDequeue
    /// are observed without sleeping. After the wake, optionally sleep
    /// <see cref="_coalesceWindowMs"/> to let more producers accumulate items.
    /// Returns false if the coalesce delay was canceled (loop should break).
    /// </summary>
    private async Task<bool> ParkUntilProducerAsync(CancellationToken ct)
    {
        await _outbound.WaitForItemsAsync(ct).ConfigureAwait(false);
        if (_coalesceWindowMs > 0)
        {
            try { await Task.Delay(_coalesceWindowMs, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return false; }
        }
        return true;
    }

    private static int CoalesceMessages(List<ReadOnlyMemory<byte>> messages, ref byte[] coalesceBuf)
    {
        int offset = 0;
        foreach (var raw in messages)
        {
            EnsureCapacity(ref coalesceBuf, offset, raw.Length);
            raw.Span.CopyTo(coalesceBuf.AsSpan(offset));
            offset += raw.Length;
        }
        return offset;
    }

    /// <summary>
    /// Pull-side dirty-flag delivery: scan all securities currently subscribed to
    /// Info updates and emit a fresh InfoSnapshot for those whose version advanced
    /// since the last cycle. Updates the per-security baseline in <c>_infoVersions</c>.
    /// Returns the number of snapshot frames appended.
    /// </summary>
    private int AppendInfoSnapshots(ref byte[] coalesceBuf, ref int offset)
    {
        if (_marketDataManagers is not { } managers) return 0;
        int infoMessages = 0;
        foreach (var (secId, lastVer) in _infoVersions)
        {
            InstrumentInfo? info = null;
            foreach (var mdm in managers)
            {
                if (mdm.InstrumentData.TryGetValue(secId, out info))
                    break;
            }
            if (info is null) continue;
            long ver = info.Version;
            if (ver <= lastVer) continue;

            EnsureCapacity(ref coalesceBuf, offset, WireProtocol.InfoSnapshotMaxSize);
            int len = WireProtocol.WriteInfoSnapshot(coalesceBuf.AsSpan(offset), secId, info);
            offset += len;
            _infoVersions[secId] = ver;
            infoMessages++;
        }
        return infoMessages;
    }

    /// <summary>
    /// Returns true when the queue depth has stayed above the slow-client threshold
    /// for <see cref="_slowClientMaxTicks"/> consecutive cycles, in which case the
    /// session is disconnected and the caller should break the write loop.
    /// </summary>
    private bool CheckSlowClient(ref int slowTicks)
    {
        int remaining = QueueDepth;
        if (remaining > ChannelCapacity * _slowClientThreshold)
        {
            if (++slowTicks >= _slowClientMaxTicks)
            {
                DisconnectSlowConsumer(
                    $"queue backlog persisted for {slowTicks} cycles (depth={remaining}/{ChannelCapacity})");
                return true;
            }
        }
        else
        {
            slowTicks = 0;
        }
        return false;
    }

    private bool TryEnqueueCore(in OutboundMessage outbound, string itemKind)
    {
        if (_cts.IsCancellationRequested)
        {
            if (outbound.PooledArray is { } pooled)
                BroadcastBufferPool.Shared.Return(pooled);
            return false;
        }

        // Bytes budget guard: payload-bearing messages count toward the cap.
        // Trip the slow-consumer disconnect immediately when accepting the new
        // payload would push us past the budget — this keeps fast producers
        // from accumulating multi-MB queues that the queue-depth threshold
        // (counted in messages) wouldn't catch in time.
        int payloadLen = outbound.Payload.Length;
        if (_maxPendingBytes > 0 && payloadLen > 0)
        {
            long current = Interlocked.Read(ref _pendingBytes);
            if (current + payloadLen > _maxPendingBytes)
            {
                if (outbound.PooledArray is { } pooledOver)
                    BroadcastBufferPool.Shared.Return(pooledOver);
                DisconnectSlowConsumer(
                    $"pending-bytes budget exceeded while enqueuing {itemKind} (pending={current}+{payloadLen} > {_maxPendingBytes})");
                return false;
            }
            Interlocked.Add(ref _pendingBytes, payloadLen);
        }

        if (_outbound.TryEnqueue(outbound))
            return true;

        // Enqueue failed: we already reserved bytes above; release them.
        if (_maxPendingBytes > 0 && payloadLen > 0)
            Interlocked.Add(ref _pendingBytes, -payloadLen);

        if (outbound.PooledArray is { } pooled2)
            BroadcastBufferPool.Shared.Return(pooled2);

        DisconnectSlowConsumer(
            $"outbound queue full while enqueuing {itemKind} (depth={QueueDepth}/{ChannelCapacity})");
        return false;
    }

    private void DrainAndReturnPooled()
    {
        while (_outbound.TryDequeue(out var outbound))
        {
            if (outbound.Payload.Length > 0 && _maxPendingBytes > 0)
                Interlocked.Add(ref _pendingBytes, -outbound.Payload.Length);
            if (outbound.PooledArray is { } pooled)
                BroadcastBufferPool.Shared.Return(pooled);
        }
    }

    /// <summary>
    /// Force-disconnect this client as a slow consumer with the given reason.
    /// Idempotent. Used by the periodic outlier sweep when the client's pending
    /// payload bytes are an outlier relative to the median active client.
    /// </summary>
    internal void DisconnectAsSlowConsumer(string reason) => DisconnectSlowConsumer(reason);

    private void DisconnectSlowConsumer(string reason)
    {
        if (Interlocked.Exchange(ref _disconnectRequested, 1) != 0)
            return;

        MetricsRegistry.WsSlowDisconnects.Add(1);
        _logger.LogWarning("Disconnecting slow client {ClientId}: {Reason}", Id, reason);
        Cancel();
    }

    private static void EnsureCapacity(ref byte[] buf, int offset, int needed)
    {
        int total = offset + needed;
        if (total <= buf.Length) return;
        int newSize = Math.Max(buf.Length * 2, total);
        var tmp = new byte[newSize];
        buf.AsSpan(0, offset).CopyTo(tmp);
        buf = tmp;
    }

    /// <summary>
    /// Read loop: reads binary WebSocket frames and yields parsed requests.
    /// Rejects frames that exceed the max client message size or arrive in fragments.
    /// </summary>
    public async IAsyncEnumerable<(MessageType type, ReadOnlyMemory<byte> payload)> ReadMessagesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // Max client message: header(4) + flags(1) + symbolLen(1) + symbol(255) = 261
        var buffer = new byte[512];
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);

        while (true)
        {
            ValueWebSocketReceiveResult result;
            try
            {
                if (Socket.State != WebSocketState.Open) break;
                result = await Socket.ReceiveAsync(buffer.AsMemory(), linked.Token);
            }
            catch (OperationCanceledException) { yield break; }
            catch (WebSocketException ex)
            {
                _logger.LogWarning("Read error on {ClientId}: {Error}", Id, ex.Message);
                yield break;
            }

            if (result.MessageType == WebSocketMessageType.Close) break;
            if (!result.EndOfMessage) continue; // discard oversized/fragmented frames
            if (result.Count < WireProtocol.FramingHeaderSize) continue;

            var span = buffer.AsSpan(0, result.Count);
            if (!WireProtocol.TryReadFramingHeader(span, out var length, out var type)) continue;
            if (length > result.Count) continue;

            var payload = buffer.AsMemory(WireProtocol.FramingHeaderSize, length - WireProtocol.FramingHeaderSize);
            yield return (type, payload);
        }
    }

    /// <summary>
    /// Send a clean WebSocket close frame (1001 Going Away by default; here we use
    /// <see cref="WebSocketCloseStatus.EndpointUnavailable"/> which maps to status
    /// code 1001) so the peer learns the server is shutting down rather than seeing
    /// a TCP RST. Idempotent: no-op once the socket has left the Open state.
    /// Bounded by the supplied <paramref name="cancellationToken"/> so the host's
    /// shutdown drain budget remains in control.
    /// </summary>
    public async Task RequestGracefulCloseAsync(string statusDescription, CancellationToken cancellationToken)
    {
        if (Socket.State != WebSocketState.Open) return;
        try
        {
            await Socket.CloseOutputAsync(
                WebSocketCloseStatus.EndpointUnavailable,
                statusDescription,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (WebSocketException ex)
        {
            _logger.LogWarning("Graceful close failed for {ClientId}: {Error}", Id, ex.Message);
        }
    }

    public void Cancel()
    {
        if (_cts.IsCancellationRequested)
            return;

        _cts.Cancel();
        // Wake the writer if it is parked on the ring so it can observe cancellation
        // and exit through the finally block (which drains any remaining pooled buffers).
        _outbound.SignalShutdown();
    }

    public void Dispose()
    {
        Cancel();
        _cts.Dispose();
        _outbound.Dispose();
    }
}
