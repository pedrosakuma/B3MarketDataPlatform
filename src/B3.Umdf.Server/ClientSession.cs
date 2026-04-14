using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Threading.Channels;
using B3.Umdf.Book;
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
    /// <summary>Book cleared event — purges pending orders for that security/side in conflation.</summary>
    BookCleared,
}

/// <summary>
/// Outbound event for the conflation channel.
/// For Passthrough, RawData carries pre-serialized bytes.
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

    public static OutboundEvent BookClear(ulong securityId, byte clearSide) =>
        new(OutboundEventKind.BookCleared, default, securityId, 0, clearSide, 0, 0);
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
    private readonly ConcurrentDictionary<ulong, long> _infoVersions = new();
    private MarketDataManager? _marketDataManager;
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger _logger;

    public IReadOnlySet<ulong> Subscriptions => _subscriptions;
    public CancellationToken CancellationToken => _cts.Token;

    public ClientSession(WebSocket socket, int channelCapacity = 65536, ILogger? logger = null)
    {
        Id = $"client-{Interlocked.Increment(ref _nextId)}";
        Socket = socket;
        ChannelCapacity = channelCapacity;
        _logger = logger ?? NullLogger.Instance;
        _outbound = Channel.CreateUnbounded<OutboundEvent>(new UnboundedChannelOptions
        {
            SingleWriter = false,
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

    /// <summary>Set the MarketDataManager reference for on-demand info reads.</summary>
    public void SetMarketDataManager(MarketDataManager mdm) =>
        _marketDataManager = mdm;

    private long _conflatedMessages;

    /// <summary>Number of messages eliminated by conflation.</summary>
    public long ConflatedMessages => Volatile.Read(ref _conflatedMessages);

    /// <summary>Current outbound queue depth.</summary>
    public int QueueDepth => _outbound.Reader.CanCount ? _outbound.Reader.Count : 0;

    /// <summary>Soft capacity reference for slow-client detection.</summary>
    public int ChannelCapacity { get; }

    // --- Enqueue methods (called from the feed thread) ---

    /// <summary>
    /// Enqueue a pre-serialized message (passthrough — control, trades, snapshots).
    /// Unbounded — never drops.
    /// </summary>
    public void TryEnqueue(ReadOnlyMemory<byte> message) =>
        _outbound.Writer.TryWrite(OutboundEvent.Passthrough(message));

    /// <summary>
    /// Enqueue a typed order event for MBO conflation.
    /// Orders with the same orderId are conflated in the write loop.
    /// </summary>
    public void TryEnqueueOrder(MessageType type, ulong securityId, ulong orderId, byte side, long price, long qty)
    {
        var evt = type switch
        {
            MessageType.OrderAdded => OutboundEvent.OrderAdd(securityId, orderId, side, price, qty),
            MessageType.OrderUpdated => OutboundEvent.OrderUpdate(securityId, orderId, side, price, qty),
            MessageType.OrderDeleted => OutboundEvent.OrderDelete(securityId, orderId, side),
            _ => throw new ArgumentException($"Invalid order message type: {type}")
        };
        _outbound.Writer.TryWrite(evt);
    }

    /// <summary>
    /// Enqueue a book cleared event. Purges pending order events for the security/side
    /// in the conflation dictionary to maintain correct ordering.
    /// </summary>
    public void TryEnqueueBookCleared(ulong securityId, byte clearSide) =>
        _outbound.Writer.TryWrite(OutboundEvent.BookClear(securityId, clearSide));

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
    /// Write loop: drains the outbound channel, conflates MBO order events,
    /// then reads dirty info via version counters, serializes and coalesces
    /// into a single WebSocket binary frame.
    /// Drain is bounded per cycle to keep frames small and the loop responsive.
    /// </summary>
    private const double SlowClientThreshold = 0.75;
    private const int SlowClientMaxTicks = 100;
    private const int MaxDrainPerCycle = 16384;

    public async Task RunWriteLoopAsync()
    {
        var ct = _cts.Token;
        var coalesceBuf = new byte[65536];

        // Reusable buffers — cleared each drain cycle
        var passthroughList = new List<ReadOnlyMemory<byte>>();
        var pendingOrders = new Dictionary<ulong, PendingOrder>();
        var bookClearList = new List<(ulong SecurityId, byte Side)>();
        int slowTicks = 0;

        try
        {
            var reader = _outbound.Reader;
            while (await reader.WaitToReadAsync(ct))
            {
                if (Socket.State != WebSocketState.Open) break;

                // 1. Drain up to MaxDrainPerCycle events with MBO order conflation
                passthroughList.Clear();
                pendingOrders.Clear();
                bookClearList.Clear();

                int drained = 0;
                while (drained < MaxDrainPerCycle && reader.TryRead(out var evt))
                {
                    drained++;
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
                        case OutboundEventKind.BookCleared:
                            PurgeOrdersForClear(pendingOrders, evt.SecurityId, evt.Side);
                            bookClearList.Add((evt.SecurityId, evt.Side));
                            break;
                    }
                }

                // 2. Serialize into coalesced buffer
                int offset = 0;

                // Passthrough (snapshots, control messages, trades)
                foreach (var raw in passthroughList)
                {
                    EnsureCapacity(ref coalesceBuf, offset, raw.Length);
                    raw.Span.CopyTo(coalesceBuf.AsSpan(offset));
                    offset += raw.Length;
                }

                // Book clears before orders
                foreach (var (secId, side) in bookClearList)
                {
                    EnsureCapacity(ref coalesceBuf, offset, 13);
                    WireProtocol.WriteBookCleared(coalesceBuf.AsSpan(offset), secId, side);
                    offset += 13;
                }

                // Conflated orders
                foreach (var (orderId, pending) in pendingOrders)
                {
                    if (pending.Kind == PendingOrderKind.Deleted)
                    {
                        EnsureCapacity(ref coalesceBuf, offset, 21);
                        WireProtocol.WriteOrderDeleted(coalesceBuf.AsSpan(offset),
                            pending.SecurityId, orderId, pending.Side);
                        offset += 21;
                    }
                    else
                    {
                        var msgType = pending.Kind == PendingOrderKind.Added
                            ? MessageType.OrderAdded : MessageType.OrderUpdated;
                        EnsureCapacity(ref coalesceBuf, offset, 37);
                        WireProtocol.WriteOrderEvent(coalesceBuf.AsSpan(offset), msgType,
                            pending.SecurityId, orderId, pending.Side, pending.Price, pending.Quantity);
                        offset += 37;
                    }
                }

                // 3. Dirty-flag info: read latest from MarketDataManager for changed securities
                int infoCount = 0;
                if (_marketDataManager is { } mdm)
                {
                    foreach (var (secId, lastVer) in _infoVersions)
                    {
                        if (!mdm.InstrumentData.TryGetValue(secId, out var info)) continue;
                        long ver = info.Version;
                        if (ver <= lastVer) continue;

                        EnsureCapacity(ref coalesceBuf, offset, WireProtocol.InfoSnapshotMaxSize);
                        int len = WireProtocol.WriteInfoSnapshot(coalesceBuf.AsSpan(offset), secId, info);
                        offset += len;
                        _infoVersions[secId] = ver;
                        infoCount++;
                    }
                }

                Interlocked.Add(ref _conflatedMessages,
                    passthroughList.Count + pendingOrders.Count + bookClearList.Count + infoCount);

                if (offset > 0)
                    await Socket.SendAsync(coalesceBuf.AsMemory(0, offset),
                        WebSocketMessageType.Binary, true, ct);

                // 4. Slow-client self-detection: if queue stays above threshold
                // after drain+send, this client can't keep up.
                int remaining = QueueDepth;
                if (remaining > ChannelCapacity * SlowClientThreshold)
                {
                    if (++slowTicks >= SlowClientMaxTicks)
                    {
                        _logger.LogWarning("Self-disconnecting {ClientId}: queue backlog persisted for {Ticks} cycles (depth={Depth}/{Capacity})",
                            Id, slowTicks, remaining, ChannelCapacity);
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

    /// <summary>Serialize a typed order event into a pre-built byte buffer.</summary>
    private static ReadOnlyMemory<byte> SerializeOrderEvent(OutboundEvent evt)
    {
        if (evt.Kind == OutboundEventKind.OrderDeleted)
        {
            var buf = new byte[21];
            WireProtocol.WriteOrderDeleted(buf, evt.SecurityId, evt.OrderId, evt.Side);
            return buf;
        }
        else
        {
            var buf = new byte[37];
            var msgType = evt.Kind == OutboundEventKind.OrderAdded
                ? MessageType.OrderAdded : MessageType.OrderUpdated;
            WireProtocol.WriteOrderEvent(buf, msgType, evt.SecurityId, evt.OrderId, evt.Side, evt.Price, evt.Quantity);
            return buf;
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

    /// <summary>
    /// Purges pending order events that would be superseded by a BookCleared.
    /// clearSide: 0=Both, 1=Bid, 2=Ask. Order side: 0=Bid, 1=Ask.
    /// </summary>
    private static void PurgeOrdersForClear(Dictionary<ulong, PendingOrder> orders, ulong securityId, byte clearSide)
    {
        List<ulong>? toRemove = null;
        foreach (var (orderId, pending) in orders)
        {
            if (pending.SecurityId != securityId) continue;
            // clearSide 0=Both, 1=Bid (matches side 0), 2=Ask (matches side 1)
            if (clearSide != 0 && pending.Side != (clearSide - 1)) continue;
            toRemove ??= new();
            toRemove.Add(orderId);
        }
        if (toRemove is not null)
            foreach (var id in toRemove)
                orders.Remove(id);
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
