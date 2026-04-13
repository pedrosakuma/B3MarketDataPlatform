using System.Net.WebSockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server;

/// <summary>Discriminator for outbound events in the conflation channel.</summary>
public enum OutboundEventKind : byte
{
    /// <summary>Pre-serialized message sent as-is (control messages, trades, snapshots).</summary>
    Passthrough,
    /// <summary>MBO order add — conflatable per orderId.</summary>
    OrderAdded,
    /// <summary>MBO order update — conflatable per orderId.</summary>
    OrderUpdated,
    /// <summary>MBO order delete — conflatable per orderId.</summary>
    OrderDeleted,
    /// <summary>Instrument info update — last-state-wins per securityId.</summary>
    InfoUpdate,
}

/// <summary>
/// Outbound event for the conflation channel.
/// For Passthrough/InfoUpdate, RawData carries pre-serialized bytes.
/// For Order* kinds, the typed fields carry the event data (serialized after conflation).
/// </summary>
public readonly struct OutboundEvent
{
    public OutboundEventKind Kind { get; }
    public ReadOnlyMemory<byte> RawData { get; }
    public ulong SecurityId { get; }
    public ulong OrderId { get; }
    public byte Side { get; }
    public long Price { get; }
    public long Quantity { get; }

    private OutboundEvent(OutboundEventKind kind, ReadOnlyMemory<byte> rawData,
        ulong securityId, ulong orderId, byte side, long price, long quantity)
    {
        Kind = kind;
        RawData = rawData;
        SecurityId = securityId;
        OrderId = orderId;
        Side = side;
        Price = price;
        Quantity = quantity;
    }

    public static OutboundEvent Passthrough(ReadOnlyMemory<byte> data) =>
        new(OutboundEventKind.Passthrough, data, 0, 0, 0, 0, 0);

    public static OutboundEvent OrderAdd(ulong securityId, ulong orderId, byte side, long price, long qty) =>
        new(OutboundEventKind.OrderAdded, default, securityId, orderId, side, price, qty);

    public static OutboundEvent OrderUpdate(ulong securityId, ulong orderId, byte side, long price, long qty) =>
        new(OutboundEventKind.OrderUpdated, default, securityId, orderId, side, price, qty);

    public static OutboundEvent OrderDelete(ulong securityId, ulong orderId, byte side) =>
        new(OutboundEventKind.OrderDeleted, default, securityId, orderId, side, 0, 0);

    public static OutboundEvent Info(ulong securityId, ReadOnlyMemory<byte> data) =>
        new(OutboundEventKind.InfoUpdate, data, securityId, 0, 0, 0, 0);
}

/// <summary>
/// Per-WebSocket-connection state. Manages subscriptions and outbound message channel.
/// </summary>
public sealed class ClientSession : IDisposable
{
    private static int _nextId;

    public string Id { get; }
    public WebSocket Socket { get; }
    private readonly Channel<OutboundEvent> _outbound;
    private readonly HashSet<ulong> _subscriptions = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger _logger;

    public IReadOnlySet<ulong> Subscriptions => _subscriptions;
    public CancellationToken CancellationToken => _cts.Token;

    public ClientSession(WebSocket socket, int channelCapacity = 4096, ILogger? logger = null)
    {
        Id = $"client-{Interlocked.Increment(ref _nextId)}";
        Socket = socket;
        ChannelCapacity = channelCapacity;
        _logger = logger ?? NullLogger.Instance;
        _outbound = Channel.CreateBounded<OutboundEvent>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
    }

    public bool IsSubscribed(ulong securityId) => _subscriptions.Contains(securityId);

    /// <summary>Add a subscription. Called on the feed thread.</summary>
    public void AddSubscription(ulong securityId) => _subscriptions.Add(securityId);

    /// <summary>Remove a subscription. Called on the feed thread or WS read thread.</summary>
    public void RemoveSubscription(ulong securityId) => _subscriptions.Remove(securityId);

    private long _droppedMessages;
    private long _conflatedMessages;

    /// <summary>Number of messages dropped due to slow consumption.</summary>
    public long DroppedMessages => Volatile.Read(ref _droppedMessages);

    /// <summary>Number of messages eliminated by conflation.</summary>
    public long ConflatedMessages => Volatile.Read(ref _conflatedMessages);

    /// <summary>Current outbound queue depth.</summary>
    public int QueueDepth => _outbound.Reader.CanCount ? _outbound.Reader.Count : 0;

    /// <summary>Channel capacity.</summary>
    public int ChannelCapacity { get; }

    // --- Enqueue methods (called from the feed thread) ---

    private bool EnqueueInternal(OutboundEvent evt)
    {
        bool wasFull = QueueDepth >= ChannelCapacity;
        _outbound.Writer.TryWrite(evt);
        if (wasFull)
        {
            Interlocked.Increment(ref _droppedMessages);
            return false;
        }
        return true;
    }

    /// <summary>
    /// Enqueue a pre-serialized message (passthrough — control, trades, snapshots).
    /// Non-blocking — drops oldest if channel is full.
    /// </summary>
    public bool TryEnqueue(ReadOnlyMemory<byte> message) =>
        EnqueueInternal(OutboundEvent.Passthrough(message));

    /// <summary>
    /// Enqueue a typed order event for MBO conflation.
    /// Orders with the same orderId are conflated in the write loop.
    /// </summary>
    public bool TryEnqueueOrder(MessageType type, ulong securityId, ulong orderId, byte side, long price, long qty)
    {
        var evt = type switch
        {
            MessageType.OrderAdded => OutboundEvent.OrderAdd(securityId, orderId, side, price, qty),
            MessageType.OrderUpdated => OutboundEvent.OrderUpdate(securityId, orderId, side, price, qty),
            MessageType.OrderDeleted => OutboundEvent.OrderDelete(securityId, orderId, side),
            _ => throw new ArgumentException($"Invalid order message type: {type}")
        };
        return EnqueueInternal(evt);
    }

    /// <summary>
    /// Enqueue a typed info update. Last-state-wins per securityId in the write loop.
    /// </summary>
    public bool TryEnqueueInfo(ulong securityId, ReadOnlyMemory<byte> serializedInfo) =>
        EnqueueInternal(OutboundEvent.Info(securityId, serializedInfo));

    // --- Write loop with conflation + coalescing ---

    private enum PendingOrderKind : byte { Added, Updated, Deleted }

    private readonly struct PendingOrder
    {
        public PendingOrderKind Kind { get; }
        public ulong SecurityId { get; }
        public byte Side { get; }
        public long Price { get; }
        public long Quantity { get; }

        public PendingOrder(PendingOrderKind kind, ulong securityId, byte side, long price, long qty)
        {
            Kind = kind; SecurityId = securityId; Side = side; Price = price; Quantity = qty;
        }
    }

    /// <summary>
    /// Write loop: drains the outbound channel, conflates MBO order events and info updates,
    /// then serializes and coalesces into a single WebSocket binary frame.
    /// </summary>
    public async Task RunWriteLoopAsync()
    {
        var ct = _cts.Token;
        var coalesceBuf = new byte[4096];

        // Reusable conflation state — cleared each drain cycle
        var passthroughList = new List<ReadOnlyMemory<byte>>();
        var pendingOrders = new Dictionary<ulong, PendingOrder>();
        var pendingInfos = new Dictionary<ulong, ReadOnlyMemory<byte>>();

        try
        {
            var reader = _outbound.Reader;
            while (await reader.WaitToReadAsync(ct))
            {
                if (Socket.State != WebSocketState.Open) break;

                // 1. Drain all available events and conflate
                passthroughList.Clear();
                pendingOrders.Clear();
                pendingInfos.Clear();
                int drainedCount = 0;

                while (reader.TryRead(out var evt))
                {
                    drainedCount++;
                    switch (evt.Kind)
                    {
                        case OutboundEventKind.Passthrough:
                            passthroughList.Add(evt.RawData);
                            break;
                        case OutboundEventKind.OrderAdded:
                        case OutboundEventKind.OrderUpdated:
                        case OutboundEventKind.OrderDeleted:
                            ConflateOrder(pendingOrders, evt);
                            break;
                        case OutboundEventKind.InfoUpdate:
                            pendingInfos[evt.SecurityId] = evt.RawData;
                            break;
                    }
                }

                // 2. Serialize into coalesced buffer
                int offset = 0;

                // Passthrough first (control messages, trades, snapshots)
                foreach (var raw in passthroughList)
                {
                    EnsureCapacity(ref coalesceBuf, offset, raw.Length);
                    raw.Span.CopyTo(coalesceBuf.AsSpan(offset));
                    offset += raw.Length;
                }

                // Order events (post-conflation)
                foreach (var (orderId, pending) in pendingOrders)
                {
                    if (pending.Kind == PendingOrderKind.Deleted)
                    {
                        const int len = 21;
                        EnsureCapacity(ref coalesceBuf, offset, len);
                        WireProtocol.WriteOrderDeleted(coalesceBuf.AsSpan(offset),
                            pending.SecurityId, orderId, pending.Side);
                        offset += len;
                    }
                    else
                    {
                        const int len = 37;
                        EnsureCapacity(ref coalesceBuf, offset, len);
                        var msgType = pending.Kind == PendingOrderKind.Added
                            ? MessageType.OrderAdded : MessageType.OrderUpdated;
                        WireProtocol.WriteOrderEvent(coalesceBuf.AsSpan(offset), msgType,
                            pending.SecurityId, orderId, pending.Side, pending.Price, pending.Quantity);
                        offset += len;
                    }
                }

                // Info updates (post-conflation — one per securityId)
                foreach (var (_, infoData) in pendingInfos)
                {
                    EnsureCapacity(ref coalesceBuf, offset, infoData.Length);
                    infoData.Span.CopyTo(coalesceBuf.AsSpan(offset));
                    offset += infoData.Length;
                }

                // Track conflation
                int emittedCount = passthroughList.Count + pendingOrders.Count + pendingInfos.Count;
                int conflated = drainedCount - emittedCount;
                if (conflated > 0)
                    Interlocked.Add(ref _conflatedMessages, conflated);

                if (offset > 0)
                    await Socket.SendAsync(coalesceBuf.AsMemory(0, offset),
                        WebSocketMessageType.Binary, true, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException ex)
        {
            _logger.LogWarning("Write error on {ClientId}: {Error}", Id, ex.Message);
        }
    }

    /// <summary>
    /// MBO order conflation state machine.
    /// Add+Delete → cancel. Add+Update → Add(new). Update+Update → Update(new).
    /// Update+Delete → Delete. Delete+Add → Update(new).
    /// </summary>
    private static void ConflateOrder(Dictionary<ulong, PendingOrder> orders, OutboundEvent evt)
    {
        if (orders.TryGetValue(evt.OrderId, out var existing))
        {
            if (evt.Kind == OutboundEventKind.OrderDeleted)
            {
                if (existing.Kind == PendingOrderKind.Added)
                    orders.Remove(evt.OrderId); // Add+Delete → cancel entirely
                else
                    orders[evt.OrderId] = new PendingOrder(PendingOrderKind.Deleted,
                        evt.SecurityId, evt.Side, 0, 0);
            }
            else
            {
                // Add or Update: preserve Add kind if original was Add
                var kind = existing.Kind == PendingOrderKind.Added
                    ? PendingOrderKind.Added : PendingOrderKind.Updated;
                orders[evt.OrderId] = new PendingOrder(kind,
                    evt.SecurityId, evt.Side, evt.Price, evt.Quantity);
            }
        }
        else
        {
            var kind = evt.Kind switch
            {
                OutboundEventKind.OrderAdded => PendingOrderKind.Added,
                OutboundEventKind.OrderUpdated => PendingOrderKind.Updated,
                _ => PendingOrderKind.Deleted,
            };
            orders[evt.OrderId] = new PendingOrder(kind,
                evt.SecurityId, evt.Side, evt.Price, evt.Quantity);
        }
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

    public void Cancel() => _cts.Cancel();

    public void Dispose()
    {
        _cts.Cancel();
        _outbound.Writer.TryComplete();
        _cts.Dispose();
    }
}
