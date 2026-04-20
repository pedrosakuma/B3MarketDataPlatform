using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Threading.Channels;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server;

/// <summary>
/// Per-WebSocket-connection state. Manages subscriptions and outbound message channel.
/// All book/trade events arrive pre-serialized from upstream conflation in SubscriptionManager.
/// The write loop coalesces pre-serialized messages + dirty-flag info into single WebSocket frames.
/// </summary>
public sealed class ClientSession : IDisposable
{
    private readonly struct OutboundMessage
    {
        public static readonly OutboundMessage InfoWake = new(ReadOnlyMemory<byte>.Empty, isInfoWake: true, logicalCount: 0);

        public OutboundMessage(ReadOnlyMemory<byte> payload, bool isInfoWake = false, int logicalCount = 1)
        {
            Payload = payload;
            IsInfoWake = isInfoWake;
            LogicalCount = logicalCount;
        }

        public ReadOnlyMemory<byte> Payload { get; }
        public bool IsInfoWake { get; }

        /// <summary>
        /// Number of logical wire messages contained in this payload. Coalesced batches
        /// from upstream conflation carry N pre-serialized messages back-to-back so the
        /// stat counters (<see cref="MessagesSent"/>, <c>WsMessagesSent</c>) reflect the
        /// true wire-event volume rather than the queue-write count.
        /// </summary>
        public int LogicalCount { get; }
    }

    private static int _nextId;

    public string Id { get; }
    public WebSocket Socket { get; }
    private readonly Channel<OutboundMessage> _outbound;
    private readonly HashSet<ulong> _subscriptions = new();
    private readonly ConcurrentDictionary<ulong, long> _infoVersions = new();
    private MarketDataManager[]? _marketDataManagers;
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger _logger;
    private readonly double _slowClientThreshold;
    private readonly int _slowClientMaxTicks;

    private long _messagesSent;
    private long _bytesSent;
    private int _disconnectRequested;
    private int _infoWakePending;

    public IReadOnlySet<ulong> Subscriptions => _subscriptions;
    public CancellationToken CancellationToken => _cts.Token;

    /// <summary>Total messages (pre-serialized + info) sent to this client.</summary>
    public long MessagesSent => Volatile.Read(ref _messagesSent);

    /// <summary>Total bytes sent to this client.</summary>
    public long BytesSent => Volatile.Read(ref _bytesSent);

    public ClientSession(
        WebSocket socket,
        int channelCapacity = 4096,
        double slowClientThreshold = 0.75,
        int slowClientMaxTicks = 100,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(socket);
        ArgumentOutOfRangeException.ThrowIfLessThan(channelCapacity, 1);
        if (slowClientThreshold is <= 0 or > 1)
            throw new ArgumentOutOfRangeException(nameof(slowClientThreshold), "Threshold must be in the (0, 1] range.");
        ArgumentOutOfRangeException.ThrowIfLessThan(slowClientMaxTicks, 1);

        Id = $"client-{Interlocked.Increment(ref _nextId)}";
        Socket = socket;
        ChannelCapacity = channelCapacity;
        _slowClientThreshold = slowClientThreshold;
        _slowClientMaxTicks = slowClientMaxTicks;
        _logger = logger ?? NullLogger.Instance;
        _outbound = Channel.CreateBounded<OutboundMessage>(new BoundedChannelOptions(channelCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false,
        });
    }

    public bool IsSubscribed(ulong securityId) => _subscriptions.Contains(securityId);

    /// <summary>Add a subscription. Called on the feed thread.</summary>
    public void AddSubscription(ulong securityId) => _subscriptions.Add(securityId);

    /// <summary>Remove a subscription. Called on the feed thread or WS read thread.</summary>
    public void RemoveSubscription(ulong securityId)
    {
        _subscriptions.Remove(securityId);
        _infoVersions.TryRemove(securityId, out _);
    }

    /// <summary>Track a security for dirty-flag info delivery. Called on the feed thread.</summary>
    public void AddInfoSubscription(ulong securityId) =>
        _infoVersions.TryAdd(securityId, 0);

    /// <summary>Set the MarketDataManagers for on-demand info reads (one per group).</summary>
    public void SetMarketDataManagers(MarketDataManager[] managers) =>
        _marketDataManagers = managers;

    /// <summary>Current outbound queue depth.</summary>
    public int QueueDepth => _outbound.Reader.CanCount ? _outbound.Reader.Count : 0;

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
    /// </summary>
    public bool TryEnqueueBatch(ReadOnlyMemory<byte> batch, int logicalMessageCount)
    {
        if (batch.IsEmpty || logicalMessageCount <= 0) return true;
        return TryEnqueueCore(new OutboundMessage(batch, logicalCount: logicalMessageCount), "batch");
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
    /// Write loop: drains the outbound channel (pre-serialized messages),
    /// reads dirty info via version counters, coalesces everything
    /// into a single WebSocket binary frame.
    /// </summary>
    public async Task RunWriteLoopAsync()
    {
        var ct = _cts.Token;
        var coalesceBuf = new byte[65536];
        var messages = new List<ReadOnlyMemory<byte>>();
        int slowTicks = 0;

        try
        {
            var reader = _outbound.Reader;
            while (await reader.WaitToReadAsync(ct))
            {
                if (Socket.State != WebSocketState.Open) break;

                // 1. Drain up to MaxDrainPerCycle pre-serialized messages
                messages.Clear();
                int drained = 0;
                int logicalDrained = 0;
                bool sawInfoWake = false;
                while (drained < MaxDrainPerCycle && reader.TryRead(out var outbound))
                {
                    drained++;
                    if (outbound.IsInfoWake)
                    {
                        sawInfoWake = true;
                        continue;
                    }

                    messages.Add(outbound.Payload);
                    logicalDrained += outbound.LogicalCount;
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
    }

    private bool TryEnqueueCore(OutboundMessage outbound, string itemKind)
    {
        if (_cts.IsCancellationRequested)
            return false;

        if (_outbound.Writer.TryWrite(outbound))
            return true;

        DisconnectSlowConsumer(
            $"outbound queue full while enqueuing {itemKind} (depth={QueueDepth}/{ChannelCapacity})");
        return false;
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

        _outbound.Writer.TryComplete();
        _cts.Cancel();
    }

    public void Dispose()
    {
        Cancel();
        _cts.Dispose();
    }
}
