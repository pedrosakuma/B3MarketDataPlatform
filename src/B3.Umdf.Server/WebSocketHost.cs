using System.Diagnostics;
using B3.Umdf.Feed;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server;

/// <summary>
/// Kestrel-based WebSocket server for market data subscriptions.
/// Includes health/readiness endpoints for orchestration compatibility.
/// AOT-compatible: uses CreateSlimBuilder and source-generated JSON.
/// </summary>
public sealed class WebSocketHost : IAsyncDisposable
{
    private readonly SubscriptionManager _subscriptionManager;
    private readonly ILogger<WebSocketHost> _logger;
    private readonly Stopwatch _uptime = Stopwatch.StartNew();
    private WebApplication? _app;
    private int _maxConnections;

    /// <summary>Optional provider for feed group states (set before StartAsync).</summary>
    public Func<IReadOnlyDictionary<string, string>>? FeedStateProvider { get; set; }

    /// <summary>Optional provider for last-packet timestamps per group.</summary>
    public Func<IReadOnlyDictionary<string, long>>? LastPacketTimestampProvider { get; set; }

    public WebSocketHost(SubscriptionManager subscriptionManager, ILogger<WebSocketHost>? logger = null, int maxConnections = 0)
    {
        _subscriptionManager = subscriptionManager;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WebSocketHost>.Instance;
        _maxConnections = maxConnections;
    }

    public async Task StartAsync(int port, CancellationToken ct = default)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
        builder.Logging.ClearProviders();
        builder.Services.ConfigureHttpJsonOptions(options =>
            options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default));

        _app = builder.Build();
        _app.UseWebSockets(new WebSocketOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(30),
        });

        // CORS for cross-origin frontend access
        _app.Use(async (context, next) =>
        {
            context.Response.Headers["Access-Control-Allow-Origin"] = "*";
            context.Response.Headers["Access-Control-Allow-Methods"] = "GET, OPTIONS";
            context.Response.Headers["Access-Control-Allow-Headers"] = "Content-Type";
            if (context.Request.Method == "OPTIONS")
            {
                context.Response.StatusCode = 204;
                return;
            }
            await next();
        });

        // Health endpoints
        _app.MapGet("/health", () =>
        {
            var result = new HealthResponse
            {
                Status = _subscriptionManager.IsReady ? "ready" : "initializing",
                Uptime = _uptime.Elapsed.ToString(@"hh\:mm\:ss"),
                ResyncCount = _subscriptionManager.ResyncCount,
            };
            if (FeedStateProvider is not null)
                result.FeedGroups = new Dictionary<string, string>(FeedStateProvider());
            if (LastPacketTimestampProvider is not null)
                result.LastPacketTimestamps = new Dictionary<string, long>(LastPacketTimestampProvider());
            return Results.Json(result, AppJsonContext.Default.HealthResponse);
        });

        _app.MapGet("/ready", () =>
            _subscriptionManager.IsReady ? Results.Ok("ready") : Results.StatusCode(503));

        _app.MapGet("/live", () => Results.Ok("alive"));

        _app.MapGet("/symbols", (string? q, int? limit) =>
        {
            var registry = _subscriptionManager.SymbolRegistry;
            if (registry is null)
                return Results.Json(new SymbolsResponse(), AppJsonContext.Default.SymbolsResponse);
            IEnumerable<string> symbols = registry.BySymbol.Keys.Order();
            if (!string.IsNullOrEmpty(q))
                symbols = symbols.Where(s => s.Contains(q, StringComparison.OrdinalIgnoreCase));
            var max = Math.Clamp(limit ?? 20, 1, 100);
            var list = symbols.Take(max).ToArray();
            return Results.Json(new SymbolsResponse { Count = registry.Count, Matched = list.Length, Symbols = list },
                AppJsonContext.Default.SymbolsResponse);
        });

        // WebSocket endpoint

        _app.Map("/ws", async context =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = 400;
                return;
            }

            // Connection limit
            if (_maxConnections > 0 && _subscriptionManager.ClientCount >= _maxConnections)
            {
                _logger.LogWarning("Connection rejected: limit {Max} reached", _maxConnections);
                context.Response.StatusCode = 503;
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
