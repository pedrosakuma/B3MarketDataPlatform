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
    private static int _nextId;

    public string Id { get; }
    public WebSocket Socket { get; }
    private readonly Channel<ReadOnlyMemory<byte>> _outbound;
    private readonly HashSet<ulong> _subscriptions = new();
    private readonly ConcurrentDictionary<ulong, long> _infoVersions = new();
    private MarketDataManager[]? _marketDataManagers;
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
        _outbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
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
    /// arrive pre-serialized from upstream conflation. Unbounded — never drops.
    /// </summary>
    public void TryEnqueue(ReadOnlyMemory<byte> message) =>
        _outbound.Writer.TryWrite(message);

    // --- Write loop: coalesce + dirty-flag info ---

    private const double SlowClientThreshold = 0.75;
    private const int SlowClientMaxTicks = 100;
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
                while (drained < MaxDrainPerCycle && reader.TryRead(out var msg))
                {
                    drained++;
                    messages.Add(msg);
                }

                // 2. Coalesce into buffer
                int offset = 0;
                foreach (var raw in messages)
                {
                    EnsureCapacity(ref coalesceBuf, offset, raw.Length);
                    raw.Span.CopyTo(coalesceBuf.AsSpan(offset));
                    offset += raw.Length;
                }

                // 3. Dirty-flag info: read latest from MarketDataManagers for changed securities
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
                    }
                }

                if (offset > 0)
                    await Socket.SendAsync(coalesceBuf.AsMemory(0, offset),
                        WebSocketMessageType.Binary, true, ct);

                // 4. Slow-client self-detection
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
