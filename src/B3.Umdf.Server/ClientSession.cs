using System.Net.WebSockets;
using System.Threading.Channels;

namespace B3.Umdf.Server;

/// <summary>
/// Per-WebSocket-connection state. Manages subscriptions and outbound message channel.
/// </summary>
public sealed class ClientSession : IDisposable
{
    private static int _nextId;

    public string Id { get; }
    public WebSocket Socket { get; }
    private readonly Channel<ReadOnlyMemory<byte>> _outbound;
    private readonly HashSet<ulong> _subscriptions = new();
    private readonly CancellationTokenSource _cts = new();

    public IReadOnlySet<ulong> Subscriptions => _subscriptions;
    public CancellationToken CancellationToken => _cts.Token;

    public ClientSession(WebSocket socket, int channelCapacity = 4096)
    {
        Id = $"client-{Interlocked.Increment(ref _nextId)}";
        Socket = socket;
        _outbound = Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(channelCapacity)
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

    /// <summary>Number of messages dropped due to slow consumption.</summary>
    public long DroppedMessages => Volatile.Read(ref _droppedMessages);

    /// <summary>
    /// Enqueue a message to be sent to the client.
    /// Non-blocking — drops oldest if channel is full.
    /// Called from the feed thread or snapshot thread.
    /// </summary>
    public bool TryEnqueue(ReadOnlyMemory<byte> message)
    {
        if (!_outbound.Writer.TryWrite(message))
        {
            Interlocked.Increment(ref _droppedMessages);
            return false;
        }
        return true;
    }

    /// <summary>
    /// Write loop: drains the outbound channel and sends binary WebSocket frames.
    /// Runs on a dedicated task per client.
    /// </summary>
    public async Task RunWriteLoopAsync()
    {
        var ct = _cts.Token;
        try
        {
            await foreach (var msg in _outbound.Reader.ReadAllAsync(ct))
            {
                if (Socket.State != WebSocketState.Open) break;
                await Socket.SendAsync(msg, WebSocketMessageType.Binary, true, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException ex)
        {
            Console.Error.WriteLine($"[ClientSession] {Id} write error: {ex.Message}");
        }
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
                Console.Error.WriteLine($"[ClientSession] {Id} read error: {ex.Message}");
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
