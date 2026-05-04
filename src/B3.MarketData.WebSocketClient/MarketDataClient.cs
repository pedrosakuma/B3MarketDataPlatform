using System.Buffers;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Typed C# subscriber for the B3MarketDataPlatform WebSocket
/// distribution layer (v1: trades, info snapshots, server status).
///
/// Lifecycle: <see cref="ConnectAsync"/> opens the WebSocket and starts
/// two background loops — one drains the socket and pushes decoded
/// events into a bounded internal channel; the other dispatches events
/// to the typed handlers. Reconnects are transparent: the SDK retries
/// with exponential back-off and re-issues every active subscription
/// when <see cref="MarketDataClientOptions.AutoResubscribeOnReconnect"/>
/// is set (default).
///
/// All event handlers are invoked sequentially on the dispatch loop
/// thread; do not block them. Handler exceptions are logged and
/// swallowed — they do not interrupt the stream.
/// </summary>
public sealed class MarketDataClient : IAsyncDisposable
{
    private readonly MarketDataClientOptions _options;
    private readonly ILogger<MarketDataClient> _logger;
    private readonly ConcurrentDictionary<string, SubscriptionRecord> _subscriptions = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<ulong, string> _securityIdToSymbol = new();
    private readonly Channel<Action> _eventChannel;
    private readonly object _stateLock = new();

    private CancellationTokenSource? _runCts;
    private Task? _runTask;
    private Task? _dispatchTask;
    private ClientWebSocket? _socket;
    private long _droppedEventCount;
    private ConnectionState _connectionState = ConnectionState.Disconnected;
    private bool _disposed;

    public MarketDataClient(MarketDataClientOptions options, ILogger<MarketDataClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Endpoint is null) throw new ArgumentException("Endpoint is required", nameof(options));
        if (options.EventChannelCapacity <= 0) throw new ArgumentOutOfRangeException(nameof(options), "EventChannelCapacity must be > 0");

        _options = options;
        _logger = logger ?? NullLogger<MarketDataClient>.Instance;

        var channelOptions = new BoundedChannelOptions(options.EventChannelCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = options.BackPressure switch
            {
                BackPressurePolicy.DropOldest => BoundedChannelFullMode.DropOldest,
                BackPressurePolicy.Block => BoundedChannelFullMode.Wait,
                BackPressurePolicy.Throw => BoundedChannelFullMode.DropWrite,
                _ => BoundedChannelFullMode.DropOldest,
            },
        };
        _eventChannel = Channel.CreateBounded<Action>(channelOptions);
    }

    // ── Public events ────────────────────────────────────────────────

    public event Action<TradeEvent>? Trade;
    public event Action<TradeBustEvent>? TradeBust;
    public event Action<InfoSnapshotEvent>? InfoSnapshot;
    public event Action<ServerStatusEvent>? ServerStatus;
    public event Action<ServerHelloEvent>? ServerHello;
    public event Action<SymbolDelistedEvent>? SymbolDelisted;
    public event Action<SubscribeErrorEvent>? SubscribeError;
    public event Action<ConnectionStateChangedEvent>? ConnectionStateChanged;

    /// <summary>
    /// Raised on the dispatch thread whenever a frame with an unrecognised
    /// <c>MessageType</c> opcode is received. The event payload is the raw opcode.
    /// Useful for forward-compat: the SDK silently skips unknown frames (so newer
    /// servers can add additive message types without breaking older clients) and
    /// surfaces them here so applications can log/alarm.
    /// </summary>
    public event Action<ushort>? UnknownMessageReceived;

    /// <summary>
    /// Cumulative number of frames received whose <c>MessageType</c> opcode the SDK
    /// did not recognise. Mirrors <see cref="UnknownMessageReceived"/> for callers
    /// that prefer polling over event subscription.
    /// </summary>
    public long UnknownMessageCount => Interlocked.Read(ref _unknownMessageCount);

    private long _unknownMessageCount;

    /// <summary>
    /// Protocol version negotiated with the server, taken from the most recent
    /// <c>ServerHello</c>. <c>null</c> until the handshake completes (or after a
    /// reconnect, before the new handshake arrives). Convenience accessor that
    /// mirrors <see cref="LastServerHello"/>.<c>ProtocolVersion</c>.
    /// </summary>
    public uint? NegotiatedProtocolVersion
    {
        get { lock (_stateLock) return _lastServerHello?.ProtocolVersion; }
    }


    /// <summary>
    /// Most recently received <c>ServerHello</c> (if any). Cached on the dispatch
    /// thread and snapshotted under <c>_stateLock</c> so callers that connect after
    /// the handshake event has already fired can still read the negotiated values.
    /// Reset to <c>null</c> on every reconnect attempt.
    /// </summary>
    public ServerHelloEvent? LastServerHello
    {
        get { lock (_stateLock) return _lastServerHello; }
    }

    private ServerHelloEvent? _lastServerHello;

    /// <summary>Current connection state. Updated by the run loop.</summary>
    public ConnectionState State
    {
        get { lock (_stateLock) return _connectionState; }
    }

    /// <summary>
    /// Cumulative number of decoded events dropped due to back-pressure
    /// (only applicable when <see cref="BackPressurePolicy.DropOldest"/>
    /// or <see cref="BackPressurePolicy.Throw"/> is in effect).
    /// </summary>
    public long DroppedEventCount => Interlocked.Read(ref _droppedEventCount);

    /// <summary>
    /// Symbols currently subscribed (snapshot — safe to enumerate).
    /// </summary>
    public IReadOnlyCollection<string> ActiveSubscriptions => _subscriptions.Keys.ToArray();

    // ── Public API ───────────────────────────────────────────────────

    /// <summary>
    /// Open the WebSocket and start the receive + dispatch loops.
    /// Returns once the underlying connection has either succeeded or
    /// the run loop has begun retrying. Subsequent calls are no-ops.
    /// </summary>
    public Task ConnectAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        lock (_stateLock)
        {
            if (_runTask is not null) return Task.CompletedTask;
            _runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _dispatchTask = Task.Run(() => DispatchLoopAsync(_runCts.Token));
            _runTask = Task.Run(() => RunLoopAsync(_runCts.Token));
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Subscribe to a symbol with the given flags (default
    /// <see cref="SubscribeFlags.Trades"/>). The Subscribe frame is
    /// queued for transmission on the current socket; the SDK also
    /// records the symbol so reconnects re-issue the subscription
    /// transparently.
    /// </summary>
    public ValueTask SubscribeAsync(string symbol, SubscribeFlags flags = SubscribeFlags.Trades, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrEmpty(symbol);
        var record = new SubscriptionRecord(symbol, flags);
        _subscriptions[symbol] = record;
        return SendSubscribeAsync(record, ct);
    }

    /// <summary>
    /// Unsubscribe by symbol. The SDK looks up the cached securityId
    /// (populated by <c>SubscribeOk</c>) and sends an <c>Unsubscribe</c>
    /// frame; if the securityId is not yet known, the entry is just
    /// removed from the active set and no frame is sent.
    /// </summary>
    public ValueTask UnsubscribeAsync(string symbol, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrEmpty(symbol);
        if (!_subscriptions.TryRemove(symbol, out var record)) return default;

        ulong secId = record.SecurityId;
        if (secId == 0) return default;
        return SendUnsubscribeAsync(secId, ct);
    }

    /// <summary>Resolve a previously-subscribed symbol to its securityId.</summary>
    public bool TryGetSecurityId(string symbol, out ulong securityId)
    {
        if (_subscriptions.TryGetValue(symbol, out var rec) && rec.SecurityId != 0)
        {
            securityId = rec.SecurityId;
            return true;
        }
        securityId = 0;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _runCts?.Cancel();
        try
        {
            if (_runTask is not null) await _runTask.ConfigureAwait(false);
        }
        catch { /* expected on cancellation */ }

        try
        {
            _eventChannel.Writer.TryComplete();
            if (_dispatchTask is not null) await _dispatchTask.ConfigureAwait(false);
        }
        catch { /* expected on cancellation */ }

        _socket?.Dispose();
        _runCts?.Dispose();
    }

    // ── Internals ────────────────────────────────────────────────────

    private async Task RunLoopAsync(CancellationToken ct)
    {
        var delay = _options.ReconnectInitialDelay;
        var rng = new Random();

        while (!ct.IsCancellationRequested)
        {
            ClientWebSocket? socket = null;
            try
            {
                ChangeState(ConnectionState.Connecting, error: null);
                socket = new ClientWebSocket();
                lock (_stateLock) _lastServerHello = null;
                await socket.ConnectAsync(_options.Endpoint, ct).ConfigureAwait(false);
                _socket = socket;
                ChangeState(ConnectionState.Connected, error: null);
                delay = _options.ReconnectInitialDelay;

                // Send ClientHello first so the server can reject incompatible
                // versions before we send Subscribe/Get frames.
                await SendClientHelloAsync(socket, ct).ConfigureAwait(false);

                if (_options.AutoResubscribeOnReconnect)
                    await ResubscribeAllAsync(ct).ConfigureAwait(false);

                await ReceiveLoopAsync(socket, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                ChangeState(ConnectionState.Reconnecting, ex);
                _logger.LogWarning(ex, "MarketDataClient connection lost; reconnecting in {DelayMs}ms", (int)delay.TotalMilliseconds);
            }
            finally
            {
                try { socket?.Dispose(); } catch { }
                if (ReferenceEquals(_socket, socket)) _socket = null;
            }

            if (ct.IsCancellationRequested) break;

            // Exponential back-off with up to 25% additive jitter.
            int jitterMs = rng.Next((int)(delay.TotalMilliseconds * 0.25) + 1);
            try
            {
                await Task.Delay(delay + TimeSpan.FromMilliseconds(jitterMs), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }

            var nextMs = Math.Min(delay.TotalMilliseconds * 2, _options.ReconnectMaxDelay.TotalMilliseconds);
            delay = TimeSpan.FromMilliseconds(nextMs);
        }

        ChangeState(ConnectionState.Disconnected, error: null);
    }

    private async Task ReceiveLoopAsync(ClientWebSocket socket, CancellationToken ct)
    {
        // 64 KiB matches the protocol's u16-length cap; anything larger
        // would be a server-side bug (and we'd close on it anyway).
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            while (!ct.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                int filled = 0;
                WebSocketReceiveResult result;
                do
                {
                    result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer, filled, buffer.Length - filled), ct).ConfigureAwait(false);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "client closing", ct).ConfigureAwait(false);
                        return;
                    }
                    filled += result.Count;
                    if (filled == buffer.Length && !result.EndOfMessage)
                        throw new InvalidOperationException("WebSocket frame exceeded 64 KiB buffer (protocol violation).");
                }
                while (!result.EndOfMessage);

                if (result.MessageType != WebSocketMessageType.Binary)
                    continue; // ignore unexpected text frames

                DispatchFrames(new ReadOnlyMemory<byte>(buffer, 0, filled));
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Decode all wire messages packed into <paramref name="data"/> and
    /// enqueue typed callbacks onto the event channel. The server may
    /// coalesce many wire messages into a single WebSocket frame
    /// (<c>UMDF_CLIENT_COALESCE_WINDOW_MS</c>) — so iterate.
    /// </summary>
    private void DispatchFrames(ReadOnlyMemory<byte> data)
    {
        var receivedUtc = DateTime.UtcNow;
        var span = data.Span;
        int offset = 0;

        while (offset < span.Length)
        {
            if (!WireFormat.TryReadHeader(span[offset..], out ushort len, out var type) || len < WireFormat.FramingHeaderSize)
            {
                _logger.LogWarning("Truncated frame header at offset {Offset}; dropping rest of packet.", offset);
                return;
            }
            if (offset + len > span.Length)
            {
                _logger.LogWarning("Truncated frame body (declared {Len}, available {Avail}); dropping rest.", len, span.Length - offset);
                return;
            }

            var payload = span.Slice(offset + WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize);
            DecodeAndEnqueue(type, payload, receivedUtc);
            offset += len;
        }
    }

    private void DecodeAndEnqueue(WireFormat.MessageType type, ReadOnlySpan<byte> payload, DateTime receivedUtc)
    {
        switch (type)
        {
            case WireFormat.MessageType.ServerStatus:
            {
                bool ready = WireFormat.ReadServerStatus(payload);
                Enqueue(() => ServerStatus?.Invoke(new ServerStatusEvent(ready, receivedUtc)));
                break;
            }
            case WireFormat.MessageType.ServerHello:
            {
                var (ver, caps, build) = WireFormat.ReadServerHello(payload);
                var ev = new ServerHelloEvent(ver, caps, build, receivedUtc);
                lock (_stateLock) _lastServerHello = ev;
                Enqueue(() => ServerHello?.Invoke(ev));
                break;
            }
            case WireFormat.MessageType.SymbolDelisted:
            {
                ulong secId = WireFormat.ReadSymbolDelisted(payload);
                string symbol = _securityIdToSymbol.TryGetValue(secId, out var s) ? s : "";
                Enqueue(() => SymbolDelisted?.Invoke(new SymbolDelistedEvent(secId, symbol, receivedUtc)));
                break;
            }
            case WireFormat.MessageType.SubscribeOk:
            {
                var (secId, _, sym) = WireFormat.ReadSubscribeOk(payload);
                _securityIdToSymbol[secId] = sym;
                if (_subscriptions.TryGetValue(sym, out var rec))
                    rec.SecurityId = secId;
                break;
            }
            case WireFormat.MessageType.SubscribeError:
            {
                var (sym, code) = WireFormat.ReadSubscribeError(payload);
                Enqueue(() => SubscribeError?.Invoke(new SubscribeErrorEvent(
                    sym,
                    Enum.IsDefined(typeof(SubscribeErrorCode), code) ? (SubscribeErrorCode)code : SubscribeErrorCode.Unknown,
                    receivedUtc)));
                break;
            }
            case WireFormat.MessageType.Trade:
            {
                var (secId, price, qty, tradeId) = WireFormat.ReadTrade(payload);
                string symbol = _securityIdToSymbol.TryGetValue(secId, out var s) ? s : "";
                var ev = new TradeEvent(secId, symbol, price / WireFormat.PriceScale, qty, tradeId, receivedUtc);
                Enqueue(() => Trade?.Invoke(ev));
                break;
            }
            case WireFormat.MessageType.TradeBust:
            {
                var (secId, tradeId) = WireFormat.ReadTradeBust(payload);
                string symbol = _securityIdToSymbol.TryGetValue(secId, out var s) ? s : "";
                Enqueue(() => TradeBust?.Invoke(new TradeBustEvent(secId, symbol, tradeId, receivedUtc)));
                break;
            }
            case WireFormat.MessageType.InfoSnapshot:
            {
                ulong secId = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(payload);
                string symbol = _securityIdToSymbol.TryGetValue(secId, out var s) ? s : "";
                // InfoSnapshot decoding allocates the event object — keep it on the
                // dispatch enqueue path; do the decode now while we still own the
                // payload buffer (the receive loop owns the rented array).
                var ev = WireFormat.ReadInfoSnapshot(payload, symbol, receivedUtc);
                Enqueue(() => InfoSnapshot?.Invoke(ev));
                break;
            }
            // Unsubscribed (0x0012) is currently silent — the application
            // already knows it called UnsubscribeAsync. Other message
            // types (Book, Mbp, News, …) are out of scope for v1 and
            // simply ignored.
            case WireFormat.MessageType.Unsubscribed:
                break;
            default:
            {
                // Forward-compat: surface unknown opcodes so applications can
                // log/alarm but keep parsing the next frame.
                Interlocked.Increment(ref _unknownMessageCount);
                ushort opcode = (ushort)type;
                Enqueue(() => UnknownMessageReceived?.Invoke(opcode));
                break;
            }
        }
    }

    private async ValueTask SendClientHelloAsync(ClientWebSocket socket, CancellationToken ct)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(WireFormat.FramingHeaderSize + 4);
        try
        {
            int len = WireFormat.WriteClientHello(buffer, WireFormat.ProtocolVersion);
            await socket.SendAsync(new ArraySegment<byte>(buffer, 0, len),
                WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void Enqueue(Action callback)
    {
        if (_eventChannel.Writer.TryWrite(callback))
            return;

        switch (_options.BackPressure)
        {
            case BackPressurePolicy.Block:
                // BoundedChannelFullMode.Wait is async; for the binary
                // flush path we synchronously spin via WaitToWriteAsync.
                _eventChannel.Writer.WriteAsync(callback).AsTask().GetAwaiter().GetResult();
                break;
            case BackPressurePolicy.Throw:
                Interlocked.Increment(ref _droppedEventCount);
                throw new InvalidOperationException("MarketDataClient event channel is full (BackPressurePolicy.Throw).");
            case BackPressurePolicy.DropOldest:
            default:
                Interlocked.Increment(ref _droppedEventCount);
                break;
        }
    }

    private async Task DispatchLoopAsync(CancellationToken ct)
    {
        try
        {
            while (await _eventChannel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (_eventChannel.Reader.TryRead(out var callback))
                {
                    try { callback(); }
                    catch (Exception ex) { _logger.LogError(ex, "MarketDataClient event handler threw"); }
                }
            }
        }
        catch (OperationCanceledException) { /* expected */ }
    }

    private async ValueTask SendSubscribeAsync(SubscriptionRecord record, CancellationToken ct)
    {
        var socket = _socket;
        if (socket is null || socket.State != WebSocketState.Open)
            return; // will be replayed by ResubscribeAllAsync after reconnect
        var buffer = ArrayPool<byte>.Shared.Rent(4 + 1 + 1 + 256);
        try
        {
            int len = WireFormat.WriteSubscribe(buffer, record.Flags, record.Symbol);
            await socket.SendAsync(new ArraySegment<byte>(buffer, 0, len), WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Subscribe send for {Symbol} failed; will be retried on reconnect.", record.Symbol);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async ValueTask SendUnsubscribeAsync(ulong securityId, CancellationToken ct)
    {
        var socket = _socket;
        if (socket is null || socket.State != WebSocketState.Open) return;
        var buffer = ArrayPool<byte>.Shared.Rent(12);
        try
        {
            int len = WireFormat.WriteUnsubscribe(buffer, securityId);
            await socket.SendAsync(new ArraySegment<byte>(buffer, 0, len), WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Unsubscribe send for {SecurityId} failed.", securityId);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async Task ResubscribeAllAsync(CancellationToken ct)
    {
        // Snapshot the current set; new subscriptions added concurrently
        // will go through SendSubscribeAsync directly.
        foreach (var record in _subscriptions.Values)
        {
            await SendSubscribeAsync(record, ct).ConfigureAwait(false);
        }
    }

    private void ChangeState(ConnectionState newState, Exception? error)
    {
        ConnectionState old;
        lock (_stateLock)
        {
            old = _connectionState;
            _connectionState = newState;
        }
        if (old == newState) return;
        try { ConnectionStateChanged?.Invoke(new ConnectionStateChangedEvent(newState, error, DateTime.UtcNow)); }
        catch (Exception ex) { _logger.LogError(ex, "ConnectionStateChanged handler threw"); }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(MarketDataClient));
    }

    private sealed class SubscriptionRecord
    {
        public string Symbol { get; }
        public SubscribeFlags Flags { get; }
        public ulong SecurityId; // populated on SubscribeOk; 0 until then

        public SubscriptionRecord(string symbol, SubscribeFlags flags)
        {
            Symbol = symbol;
            Flags = flags;
        }
    }
}
