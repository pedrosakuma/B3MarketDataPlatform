using System.Diagnostics;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server;

/// <summary>
/// Kestrel-based WebSocket server for market data subscriptions.
/// Includes health/readiness endpoints for orchestration compatibility.
/// </summary>
public sealed class WebSocketHost : IAsyncDisposable
{
    private readonly SubscriptionManager _subscriptionManager;
    private readonly ILogger<WebSocketHost> _logger;
    private readonly Stopwatch _uptime = Stopwatch.StartNew();
    private WebApplication? _app;

    /// <summary>Optional provider for feed group states (set before StartAsync).</summary>
    public Func<IReadOnlyDictionary<string, string>>? FeedStateProvider { get; set; }

    public WebSocketHost(SubscriptionManager subscriptionManager, ILogger<WebSocketHost>? logger = null)
    {
        _subscriptionManager = subscriptionManager;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WebSocketHost>.Instance;
    }

    public async Task StartAsync(int port, CancellationToken ct = default)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
        builder.Logging.ClearProviders();

        _app = builder.Build();
        _app.UseWebSockets(new WebSocketOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(30),
        });

        // Health endpoints
        _app.MapGet("/health", () =>
        {
            var result = new Dictionary<string, object>
            {
                ["status"] = _subscriptionManager.IsReady ? "ready" : "initializing",
                ["uptime"] = _uptime.Elapsed.ToString(@"hh\:mm\:ss"),
                ["slowClientDisconnects"] = _subscriptionManager.SlowClientDisconnects,
            };
            if (FeedStateProvider is not null)
                result["feedGroups"] = FeedStateProvider();
            return Results.Json(result);
        });

        _app.MapGet("/ready", () =>
            _subscriptionManager.IsReady ? Results.Ok("ready") : Results.StatusCode(503));

        _app.MapGet("/live", () => Results.Ok("alive"));

        _app.Map("/ws", async context =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = 400;
                return;
            }

            var ws = await context.WebSockets.AcceptWebSocketAsync();
            var session = new ClientSession(ws);
            _subscriptionManager.RegisterClient(session);

            _logger.LogInformation("Client {ClientId} connected", session.Id);

            // Start write loop
            var writeTask = Task.Run(() => session.RunWriteLoopAsync());

            // Read loop
            try
            {
                await foreach (var (type, payload) in session.ReadMessagesAsync(ct))
                {
                    switch (type)
                    {
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
                session.Dispose();
                await writeTask;
            }
        });

        await _app.StartAsync(ct);
        _logger.LogInformation("WebSocket server listening on port {Port}", port);
    }

    public async Task StopAsync()
    {
        if (_app is not null)
            await _app.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
            await _app.DisposeAsync();
    }
}
