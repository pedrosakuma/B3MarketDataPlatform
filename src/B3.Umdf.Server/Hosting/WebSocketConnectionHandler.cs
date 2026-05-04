using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server.Hosting;

/// <summary>
/// Owns the <c>/ws</c> endpoint: shutdown gating, connection-cap enforcement,
/// session construction, and the read/write loop pair per accepted client.
/// Extracted from <see cref="WebSocketHost"/> so the lifecycle logic is no
/// longer tangled with hosting setup, and so future protocol additions land
/// in one place.
/// </summary>
internal sealed class WebSocketConnectionHandler
{
    private readonly SubscriptionManager _subscriptionManager;
    private readonly ILogger _logger;
    private readonly ClientSessionOptions _sessionOptions;
    private readonly int _maxConnections;
    private readonly Func<bool> _isShuttingDown;

    public WebSocketConnectionHandler(
        SubscriptionManager subscriptionManager,
        ILogger logger,
        ClientSessionOptions sessionOptions,
        int maxConnections,
        Func<bool> isShuttingDown)
    {
        _subscriptionManager = subscriptionManager;
        _logger = logger;
        _sessionOptions = sessionOptions;
        _maxConnections = maxConnections;
        _isShuttingDown = isShuttingDown;
    }

    public void Map(WebApplication app, CancellationToken ct)
    {
        app.Map("/ws", async context => await HandleAsync(context, ct).ConfigureAwait(false));
    }

    private async Task HandleAsync(HttpContext context, CancellationToken ct)
    {
        // Once shutdown has been signaled, refuse new connections immediately so the
        // drain phase does not have to wait on freshly-accepted clients (and so we
        // don't keep logging the connect/disconnect churn from clients that retry).
        if (_isShuttingDown() || ct.IsCancellationRequested)
        {
            _logger.LogInformation("Connection rejected: server is shutting down");
            context.Response.StatusCode = 503;
            context.Response.Headers["Connection"] = "close";
            return;
        }

        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = 400;
            return;
        }

        // Soft connection limit: this check is intentionally racy (TOCTOU vs
        // RegisterClient below) — under heavy concurrent connect bursts we may
        // briefly exceed _maxConnections by a handful, which is acceptable.
        // Tightening would require a CAS-counter in SubscriptionManager and is
        // not worth the contention for a soft cap.
        if (_maxConnections > 0 && _subscriptionManager.ClientCount >= _maxConnections)
        {
            _logger.LogWarning("Connection rejected: limit {Max} reached", _maxConnections);
            context.Response.StatusCode = 503;
            return;
        }

        var ws = await context.WebSockets.AcceptWebSocketAsync();
        var session = new ClientSession(
            ws,
            channelCapacity: _sessionOptions.ChannelCapacity,
            slowClientThreshold: _sessionOptions.SlowClientThreshold,
            slowClientMaxTicks: _sessionOptions.SlowClientMaxTicks,
            maxPendingBytes: _sessionOptions.MaxPendingBytes,
            coalesceWindowMs: _sessionOptions.CoalesceWindowMs,
            logger: _logger);
        _subscriptionManager.RegisterClient(session);

        _logger.LogInformation("Client {ClientId} connected", session.Id);

        var writeTask = Task.Run(() => session.RunWriteLoopAsync());

        try
        {
            await foreach (var (type, payload) in session.ReadMessagesAsync(ct))
            {
                switch (type)
                {
                    case MessageType.ClientHello:
                    {
                        // Optional negotiation: clients that never send ClientHello are
                        // assumed to speak the current ProtocolVersion. If sent and
                        // incompatible, close with WS 1003 (unsupported data).
                        if (payload.Length < 4) break;
                        uint clientVer = WireProtocol.ReadClientHello(payload.Span);
                        if (clientVer < WireProtocol.SupportedProtocolVersionMin ||
                            clientVer > WireProtocol.SupportedProtocolVersionMax)
                        {
                            var reason =
                                $"protocol_version_unsupported: client {clientVer}, " +
                                $"server min {WireProtocol.SupportedProtocolVersionMin} " +
                                $"max {WireProtocol.SupportedProtocolVersionMax}";
                            _logger.LogWarning(
                                "Client {ClientId} sent unsupported ClientHello version {Version}; closing",
                                session.Id, clientVer);
                            try
                            {
                                await ws.CloseAsync(
                                    System.Net.WebSockets.WebSocketCloseStatus.InvalidMessageType,
                                    reason,
                                    CancellationToken.None).ConfigureAwait(false);
                            }
                            catch { /* best effort */ }
                            return;
                        }
                        break;
                    }

                    case MessageType.Subscribe:
                    {
                        var (symbol, flags) = WireProtocol.ReadSubscribe(payload.Span);
                        _subscriptionManager.RequestSubscribe(session.Id, symbol, flags);
                        break;
                    }

                    case MessageType.Get:
                    {
                        var (symbol, flags) = WireProtocol.ReadSubscribe(payload.Span);
                        _subscriptionManager.RequestGet(session.Id, symbol, flags);
                        break;
                    }

                    case MessageType.Unsubscribe:
                        var securityId = WireProtocol.ReadUnsubscribe(payload.Span);
                        _subscriptionManager.RequestUnsubscribe(session.Id, securityId);
                        break;
                }
            }
        }
        finally
        {
            _logger.LogInformation("Client {ClientId} disconnected", session.Id);
            _subscriptionManager.UnregisterClient(session.Id);
            session.Cancel();
            await writeTask;
            session.Dispose();
        }
    }
}
