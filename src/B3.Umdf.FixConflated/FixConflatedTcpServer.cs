using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.FixConflated;

public sealed class FixConflatedTcpServer : IAsyncDisposable
{
    private readonly FixConflatedSessionHub _hub;
    private readonly FixConflatedTcpServerOptions _options;
    private readonly FixSessionStateStore _stateStore = new();
    private readonly Func<IEnumerable<FixMessage>>? _initialMessagesProvider;
    private readonly FixMarketDataRequestHandler? _marketDataRequestHandler;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<FixConflatedTcpServer> _logger;
    private readonly ConcurrentDictionary<long, FixTcpClientSession> _sessions = new();
    private readonly CancellationTokenSource _cts = new();
    private TcpListener? _listener;
    private Task? _acceptLoop;
    private long _nextSessionId;

    public FixConflatedTcpServer(
        FixConflatedSessionHub hub,
        FixConflatedTcpServerOptions? options = null,
        Func<IEnumerable<FixMessage>>? initialMessagesProvider = null,
        FixMarketDataRequestHandler? marketDataRequestHandler = null,
        ILoggerFactory? loggerFactory = null,
        ILogger<FixConflatedTcpServer>? logger = null)
    {
        _hub = hub ?? throw new ArgumentNullException(nameof(hub));
        _options = options ?? new FixConflatedTcpServerOptions();
        _initialMessagesProvider = initialMessagesProvider;
        _marketDataRequestHandler = marketDataRequestHandler;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = logger ?? (_loggerFactory == NullLoggerFactory.Instance
            ? NullLogger<FixConflatedTcpServer>.Instance
            : _loggerFactory.CreateLogger<FixConflatedTcpServer>());
    }

    public int Port { get; private set; }

    public Task StartAsync(int port, CancellationToken cancellationToken = default)
    {
        if (port is < 1 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(port));
        if (_listener is not null)
            throw new InvalidOperationException("FIX conflated TCP server already started.");

        _listener = new TcpListener(IPAddress.Any, port);
        _listener.Start(_options.AcceptBacklog);
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        _acceptLoop = Task.Run(() => AcceptLoopAsync(linkedCts.Token), linkedCts.Token);
        _logger.LogInformation("FIX conflated TCP server listening on port {Port}", Port);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _cts.Cancel();
        _listener?.Stop();

        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
        }

        foreach (FixTcpClientSession session in _sessions.Values)
            await session.DisposeAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        TcpListener listener = _listener ?? throw new InvalidOperationException("Listener not started.");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client = await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                client.NoDelay = true;
                long sessionId = Interlocked.Increment(ref _nextSessionId);
                var session = new FixTcpClientSession(
                    sessionId,
                    client,
                    new FixSessionConnection(_stateStore, _options.SessionOptions),
                    _options.OutboundQueueCapacity,
                    _initialMessagesProvider,
                    _marketDataRequestHandler,
                    OnSessionClosed,
                    _loggerFactory.CreateLogger<FixTcpClientSession>());

                _sessions[sessionId] = session;
                _hub.Register(session);
                FixConflatedMetrics.ActiveConnections.Add(1);
                _logger.LogInformation("Accepted FIX conflated TCP session {SessionId} from {RemoteEndPoint}",
                    sessionId,
                    client.Client.RemoteEndPoint);
                session.Start();
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (SocketException ex) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "FIX TCP accept loop cancelled");
        }
    }

    private void OnSessionClosed(long sessionId)
    {
        if (_sessions.TryRemove(sessionId, out FixTcpClientSession? session))
        {
            _hub.Unregister(sessionId);
            FixConflatedMetrics.ActiveConnections.Add(-1);
            _logger.LogInformation("Closed FIX conflated TCP session {SessionId}", sessionId);
            _ = Task.Run(async () => await session.DisposeAsync().ConfigureAwait(false));
        }
    }
}
