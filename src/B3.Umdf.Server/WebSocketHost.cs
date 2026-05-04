using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.Server.Hosting;
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
///
/// This type orchestrates the host lifecycle and owns the providers wired
/// in by the ConsoleApp. The actual endpoint logic, CORS, health staleness
/// evaluation, WebSocket connection handling, and shutdown drain live in
/// dedicated classes under <see cref="Hosting"/> so each concern is
/// independently testable.
/// </summary>
public sealed class WebSocketHost : IAsyncDisposable
{
    private readonly SubscriptionManager _subscriptionManager;
    private readonly ILogger<WebSocketHost> _logger;
    private readonly Stopwatch _uptime = Stopwatch.StartNew();
    private readonly ClientSessionOptions _sessionOptions;
    private readonly int _maxConnections;
    private WebApplication? _app;
    private volatile bool _isShuttingDown;
    private CancellationTokenRegistration _shutdownRegistration;

    /// <summary>Optional provider for feed group states (set before StartAsync).</summary>
    public Func<IReadOnlyDictionary<string, string>>? FeedStateProvider { get; set; }

    /// <summary>Optional provider for last-packet timestamps per group.</summary>
    public Func<IReadOnlyDictionary<string, long>>? LastPacketTimestampProvider { get; set; }

    /// <summary>
    /// Optional provider for the recovery audit trail. When set, exposes
    /// <c>GET /api/recovery/recent?limit=N</c> returning a JSON snapshot
    /// of the most recent recovery events (newest first).
    /// </summary>
    public Func<int, IReadOnlyList<RecoveryEvent>>? RecoveryEventProvider { get; set; }

    /// <summary>Optional accessor for total events recorded since process start (for the audit-trail endpoint).</summary>
    public Func<long>? RecoveryEventTotalProvider { get; set; }

    /// <summary>
    /// Threshold (seconds) beyond which a feed group reported in a non-Streaming
    /// state by <see cref="FeedStateProvider"/> causes <c>GET /health</c> to
    /// return HTTP 503. Staleness is measured from the group's last observed
    /// packet (via <see cref="LastPacketTimestampProvider"/>); for groups that
    /// have never received a packet, process uptime is used so cold starts
    /// don't fail readiness gates until the threshold elapses. Defaults to
    /// 60 s. Mirrors <c>AppSettings.HealthMaxStaleSeconds</c>.
    /// </summary>
    public int HealthMaxStaleSeconds { get; set; } = 60;

    /// <summary>
    /// Master switch for the <c>/health</c> stale-recovery → 503 behavior.
    /// When false, the endpoint preserves its legacy always-200 contract
    /// (status + per-group state in body) regardless of staleness. Defaults
    /// to true. Mirrors <c>AppSettings.HealthFailOnRecovery</c>.
    /// </summary>
    public bool HealthFailOnRecovery { get; set; } = true;

    /// <summary>
    /// Additional <c>System.Diagnostics.Metrics.Meter</c> names to expose via
    /// the Prometheus <c>/metrics</c> endpoint. The host always exposes the
    /// <c>"B3.Umdf"</c> meter (defined by <see cref="MetricsRegistry"/>);
    /// callers may add others (e.g. <c>"B3.Umdf.Consumer"</c> registered by
    /// the ConsoleApp's <c>MetricsBinder</c>) to surface them on the same
    /// scrape endpoint. Set before <see cref="StartAsync"/>; mutations after
    /// the host has started are ignored.
    /// </summary>
    public IReadOnlyList<string> AdditionalMeterNames { get; set; } = Array.Empty<string>();

    public WebSocketHost(
        SubscriptionManager subscriptionManager,
        ILogger<WebSocketHost>? logger = null,
        int maxConnections = 0,
        int clientChannelCapacity = 4096,
        double slowClientThreshold = 0.75,
        int slowClientMaxTicks = 100,
        long clientMaxPendingBytes = 16L * 1024 * 1024,
        int clientCoalesceWindowMs = 0)
    {
        _subscriptionManager = subscriptionManager;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WebSocketHost>.Instance;
        _maxConnections = maxConnections;
        _sessionOptions = new ClientSessionOptions(
            ChannelCapacity: clientChannelCapacity,
            SlowClientThreshold: slowClientThreshold,
            SlowClientMaxTicks: slowClientMaxTicks,
            MaxPendingBytes: clientMaxPendingBytes,
            CoalesceWindowMs: clientCoalesceWindowMs);
    }

    public async Task StartAsync(int port, CancellationToken ct = default)
    {
        // Register an explicit shutdown flag toggled by the cancellation token. We don't
        // rely on capturing `ct` in the request handler closure because some hosting paths
        // make the captured struct surprisingly hard to observe; a plain volatile bool is
        // unambiguous and cheap.
        _shutdownRegistration = ct.Register(static state =>
        {
            var host = (WebSocketHost)state!;
            host._isShuttingDown = true;
            host._logger.LogInformation("WebSocket host marked as shutting down — new connections will be rejected with 503");
        }, this);

        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
        builder.Logging.ClearProviders();
        builder.Services.ConfigureHttpJsonOptions(options =>
            options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default));

        MetricsEndpointMapper.AddOpenTelemetryWithMeters(builder.Services, AdditionalMeterNames);

        _app = builder.Build();
        _app.UseWebSockets(new WebSocketOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(30),
        });

        _app.UsePermissiveCors();

        var healthMapper = new HealthEndpointMapper(
            _subscriptionManager,
            _uptime,
            () => HealthMaxStaleSeconds,
            () => HealthFailOnRecovery,
            () => FeedStateProvider,
            () => LastPacketTimestampProvider);
        healthMapper.Map(_app);

        MetricsEndpointMapper.MapPrometheus(_app);

        new SymbolEndpointMapper(_subscriptionManager).Map(_app);
        new InstrumentEndpointMapper(_subscriptionManager).Map(_app);
        new RecoveryEndpointMapper(
            () => RecoveryEventProvider,
            () => RecoveryEventTotalProvider).Map(_app);

        var wsHandler = new WebSocketConnectionHandler(
            _subscriptionManager,
            _logger,
            _sessionOptions,
            _maxConnections,
            () => _isShuttingDown);
        wsHandler.Map(_app, ct);

        await _app.StartAsync(ct);
        _logger.LogInformation("WebSocket server listening on port {Port}", port);
    }

    /// <summary>
    /// Gracefully stop the host. Before tearing down Kestrel we send a clean
    /// WebSocket close frame (1001 / EndpointUnavailable) to every active client
    /// so they observe an orderly shutdown rather than a connection reset. The
    /// per-client close handshake is bounded by <paramref name="closeHandshakeBudget"/>
    /// (defaults to 5 seconds when unset) so a misbehaving peer cannot hold the
    /// shutdown open indefinitely.
    /// </summary>
    public async Task StopAsync(TimeSpan closeHandshakeBudget = default)
    {
        if (_app is null) return;

        var coordinator = new ShutdownCoordinator(_subscriptionManager, _logger);
        await coordinator.DrainClientsAsync(closeHandshakeBudget);

        await _app.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _shutdownRegistration.Dispose();
        if (_app is not null)
            await _app.DisposeAsync();
    }
}
