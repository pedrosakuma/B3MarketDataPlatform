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

    private long _messagesSent;
    private long _bytesSent;
    private long _pendingBytes;
    private int _disconnectRequested;
    private int _infoWakePending;

    public IReadOnlySet<ulong> Subscriptions => _subscriptions;
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

    public ClientSession(
        WebSocket socket,
        int channelCapacity = 4096,
        double slowClientThreshold = 0.75,
        int slowClientMaxTicks = 100,
        long maxPendingBytes = 16L * 1024 * 1024,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(socket);
        ArgumentOutOfRangeException.ThrowIfLessThan(channelCapacity, 1);
        if (slowClientThreshold is <= 0 or > 1)
            throw new ArgumentOutOfRangeException(nameof(slowClientThreshold), "Threshold must be in the (0, 1] range.");
        ArgumentOutOfRangeException.ThrowIfLessThan(slowClientMaxTicks, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(maxPendingBytes);

        Id = $"client-{Interlocked.Increment(ref _nextId)}";
        Socket = socket;
        ChannelCapacity = channelCapacity;
        _slowClientThreshold = slowClientThreshold;
        _slowClientMaxTicks = slowClientMaxTicks;
        _maxPendingBytes = maxPendingBytes;
        _logger = logger ?? NullLogger.Instance;
        // Lock-free MPSC ring rounds capacity up to the next power of two; the
        // user-facing ChannelCapacity stays as the requested value (used as the
        // slow-client threshold denominator and for back-compat with tests).
        _outbound = new MpscOutboundRing(channelCapacity);
    }

    public bool IsSubscribed(ulong securityId) => _subscriptions.Contains(securityId);

    /// <summary>Add a subscription. Called on the feed thread.</summary>
    public void AddSubscription(ulong securityId) => _subscriptions.Add(securityId);

    /// <summary>Remove a subscription. Called on the feed thread or WS read thread.
    /// The HashSet remove stays inline (legacy single-thread access pattern); the
    /// info-version delete is routed through the outbound ring so the WriteLoop is
    /// the sole writer to <c>_infoVersions</c>.</summary>
    public void RemoveSubscription(ulong securityId)
    {
        _subscriptions.Remove(securityId);
        TryEnqueueCore(OutboundMessage.RemoveInfoSub(securityId), "remove-info-sub");
    }

    /// <summary>Track a security for dirty-flag info delivery. Called on the feed thread.
    /// Routes through the outbound ring so <c>_infoVersions</c> remains a plain
    /// (lock-free) <see cref="Dictionary{TKey, TValue}"/> mutated only by the WriteLoop.</summary>
    public void AddInfoSubscription(ulong securityId) =>
        TryEnqueueCore(OutboundMessage.AddInfoSub(securityId), "add-info-sub");

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
    /// session: the write loop returns it to <see cref="ArrayPool{T}.Shared"/> after
    /// the WS frame is sent. If the channel is full and the client is disconnected,
    /// the array is returned synchronously here. Empty/zero-count batches are no-ops
    /// and the array (if any) is returned immediately.
    /// </summary>
    public bool TryEnqueueBatch(ReadOnlyMemory<byte> batch, int logicalMessageCount, byte[]? pooledArray = null)
    {
        if (batch.IsEmpty || logicalMessageCount <= 0)
        {
            if (pooledArray is not null) ArrayPool<byte>.Shared.Return(pooledArray);
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

                // 1. Drain up to MaxDrainPerCycle pre-serialized messages
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

                if (_maxPendingBytes > 0 && payloadBytesDrained > 0)
                    Interlocked.Add(ref _pendingBytes, -payloadBytesDrained);

                if (drained == 0)
                {
                    // Park until a producer enqueues. The ring's WaitForItemsAsync
                    // re-checks after publishing the waiting flag, so producers that
                    // raced our last failed TryDequeue are observed without sleeping.
                    await _outbound.WaitForItemsAsync(ct).ConfigureAwait(false);
                    continue;
                }

                if (sawInfoWake)
                    Interlocked.Exchange(ref _infoWakePending, 0);

                // 2. Coalesce into buffer
                int offset = 0;
                foreach (var raw in messages)
                {
                    EnsureCapacity(ref coalesceBuf, offset, raw.Length);
                    raw.Span.CopyTo(coalesceBuf.AsSpan(offset));
                    offset += raw.Length;
                }

                // 3. Dirty-flag info: read latest from MarketDataManagers for changed securities
                int infoMessages = 0;
                if (_marketDataManagers is { } managers)
                {
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
                }

                if (offset > 0)
                {
                    await Socket.SendAsync(coalesceBuf.AsMemory(0, offset),
                        WebSocketMessageType.Binary, true, ct);
                    int totalMessages = logicalDrained + infoMessages;
                    Interlocked.Add(ref _messagesSent, totalMessages);
                    Interlocked.Add(ref _bytesSent, offset);
                    AppMetrics.WsMessagesSent.Add(totalMessages);
                }

                // Return pooled producer buffers AFTER SendAsync completes (Payload spans
                // were copied into coalesceBuf, but returning earlier would invalidate the
                // backing arrays held in `messages` if SendAsync inspected them later).
                for (int i = 0; i < pooledToReturn.Count; i++)
                    ArrayPool<byte>.Shared.Return(pooledToReturn[i]);

                // 4. Slow-client self-detection
                int remaining = QueueDepth;
                if (remaining > ChannelCapacity * _slowClientThreshold)
                {
                    if (++slowTicks >= _slowClientMaxTicks)
                    {
                        DisconnectSlowConsumer(
                            $"queue backlog persisted for {slowTicks} cycles (depth={remaining}/{ChannelCapacity})");
                        break;
                    }
                }
                else
                {
                    slowTicks = 0;
                }
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
            DrainAndReturnPooled();
        }
    }

    private bool TryEnqueueCore(in OutboundMessage outbound, string itemKind)
    {
        if (_cts.IsCancellationRequested)
        {
            if (outbound.PooledArray is { } pooled)
                ArrayPool<byte>.Shared.Return(pooled);
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
                    ArrayPool<byte>.Shared.Return(pooledOver);
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
            ArrayPool<byte>.Shared.Return(pooled2);

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
                ArrayPool<byte>.Shared.Return(pooled);
        }
    }

    private void DisconnectSlowConsumer(string reason)
    {
        if (Interlocked.Exchange(ref _disconnectRequested, 1) != 0)
            return;

        AppMetrics.WsSlowDisconnects.Add(1);
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
