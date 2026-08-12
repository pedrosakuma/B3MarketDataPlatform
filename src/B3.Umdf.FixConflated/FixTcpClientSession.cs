using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.FixConflated;

public sealed class FixTcpClientSession : IAsyncDisposable
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly FixSessionConnection _session;
    private readonly Func<IEnumerable<FixMessage>>? _initialMessagesProvider;
    private readonly Action<long> _onClosed;
    private readonly ILogger<FixTcpClientSession> _logger;
    private readonly Channel<byte[]> _outbound;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _sessionGate = new();
    private readonly ConcurrentBag<Task> _tasks = new();
    private int _closed;

    public FixTcpClientSession(
        long id,
        TcpClient client,
        FixSessionConnection session,
        int outboundQueueCapacity,
        Func<IEnumerable<FixMessage>>? initialMessagesProvider,
        Action<long> onClosed,
        ILogger<FixTcpClientSession>? logger = null)
    {
        if (outboundQueueCapacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(outboundQueueCapacity));

        Id = id;
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _stream = client.GetStream();
        _session = session ?? throw new ArgumentNullException(nameof(session));
        _initialMessagesProvider = initialMessagesProvider;
        _onClosed = onClosed ?? throw new ArgumentNullException(nameof(onClosed));
        _logger = logger ?? NullLogger<FixTcpClientSession>.Instance;
        _outbound = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(outboundQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
    }

    public long Id { get; }

    public void Start()
    {
        _tasks.Add(Task.Run(ReadLoopAsync));
        _tasks.Add(Task.Run(WriteLoopAsync));
        _tasks.Add(Task.Run(HeartbeatLoopAsync));
    }

    public bool TrySendApplication(FixMessage applicationMessage)
    {
        ArgumentNullException.ThrowIfNull(applicationMessage);
        if (_cts.IsCancellationRequested)
            return false;

        FixSessionUpdate update;
        lock (_sessionGate)
        {
            if (!_session.TrySendApplication(applicationMessage, out update))
                return false;
        }

        return ProcessUpdate(update);
    }

    public async ValueTask DisposeAsync()
    {
        Close();
        try
        {
            await Task.WhenAll(_tasks.ToArray()).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (IOException)
        {
        }
        catch (SocketException)
        {
        }
        finally
        {
            _stream.Dispose();
            _client.Dispose();
            _cts.Dispose();
        }
    }

    private async Task ReadLoopAsync()
    {
        byte[] buffer = new byte[16 * 1024];
        int buffered = 0;

        try
        {
            while (!_cts.IsCancellationRequested)
            {
                if (buffered == buffer.Length)
                    Array.Resize(ref buffer, buffer.Length * 2);

                int read = await _stream.ReadAsync(buffer.AsMemory(buffered), _cts.Token).ConfigureAwait(false);
                if (read == 0)
                    break;

                buffered += read;
                int consumed = 0;
                while (consumed < buffered)
                {
                    FixDecodeResult decoded = FixMessageCodec.Decode(buffer.AsSpan(consumed, buffered - consumed));
                    if (decoded.Error == FixDecodeError.Incomplete)
                        break;
                    if (!decoded.Success || decoded.Message is null)
                    {
                        _logger.LogWarning("Disconnecting FIX session {SessionId}: invalid inbound frame ({Error})", Id, decoded.Error);
                        Close();
                        return;
                    }

                    bool activated = false;
                    FixSessionUpdate update;
                    lock (_sessionGate)
                    {
                        FixSessionState previous = _session.State;
                        update = _session.Receive(decoded.Message);
                        activated = previous != FixSessionState.Active && _session.State == FixSessionState.Active;
                    }

                    if (!ProcessUpdate(update))
                        return;

                    if (activated)
                        SendInitialMessages();

                    consumed += decoded.BytesConsumed;
                }

                if (consumed == 0)
                    continue;

                Buffer.BlockCopy(buffer, consumed, buffer, 0, buffered - consumed);
                buffered -= consumed;
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (IOException ex)
        {
            _logger.LogDebug(ex, "FIX session {SessionId} read loop ended with I/O error", Id);
        }
        catch (SocketException ex)
        {
            _logger.LogDebug(ex, "FIX session {SessionId} read loop ended with socket error", Id);
        }
        finally
        {
            Close();
        }
    }

    private async Task WriteLoopAsync()
    {
        try
        {
            while (await _outbound.Reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                while (_outbound.Reader.TryRead(out byte[]? payload))
                {
                    await _stream.WriteAsync(payload, _cts.Token).ConfigureAwait(false);
                    FixConflatedMetrics.MessagesSent.Add(1);
                    FixConflatedMetrics.BytesSent.Add(payload.Length);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (IOException ex)
        {
            _logger.LogDebug(ex, "FIX session {SessionId} write loop ended with I/O error", Id);
        }
        catch (SocketException ex)
        {
            _logger.LogDebug(ex, "FIX session {SessionId} write loop ended with socket error", Id);
        }
        finally
        {
            Close();
        }
    }

    private async Task HeartbeatLoopAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(_cts.Token).ConfigureAwait(false))
            {
                FixSessionUpdate update;
                lock (_sessionGate)
                    update = _session.Advance();

                if (!ProcessUpdate(update))
                    return;
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            Close();
        }
    }

    private bool ProcessUpdate(FixSessionUpdate update)
    {
        foreach (FixMessage outbound in update.OutboundMessages)
        {
            if (!_outbound.Writer.TryWrite(FixMessageCodec.Encode(outbound)))
            {
                _logger.LogWarning("Disconnecting FIX session {SessionId}: outbound queue full", Id);
                Close();
                return false;
            }
        }

        if (update.DisconnectTransport)
        {
            Close();
            return false;
        }

        return true;
    }

    private void SendInitialMessages()
    {
        if (_initialMessagesProvider is null)
            return;

        foreach (FixMessage message in _initialMessagesProvider())
        {
            if (!TrySendApplication(message))
                return;
        }
    }

    private void Close()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;

        _cts.Cancel();
        _outbound.Writer.TryComplete();
        try { _client.Client.Shutdown(SocketShutdown.Both); }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
        _onClosed(Id);
    }
}
