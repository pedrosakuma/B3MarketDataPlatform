using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;

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
    private volatile bool _isShuttingDown;
    private CancellationTokenRegistration _shutdownRegistration;
    private readonly int _maxConnections;
    private readonly int _clientChannelCapacity;
    private readonly double _slowClientThreshold;
    private readonly int _slowClientMaxTicks;
    private readonly long _clientMaxPendingBytes;
    private readonly int _clientCoalesceWindowMs;

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
        _clientChannelCapacity = clientChannelCapacity;
        _slowClientThreshold = slowClientThreshold;
        _slowClientMaxTicks = slowClientMaxTicks;
        _clientMaxPendingBytes = clientMaxPendingBytes;
        _clientCoalesceWindowMs = clientCoalesceWindowMs;
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

        builder.Services
            .AddOpenTelemetry()
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(MetricsRegistry.Meter.Name);
                foreach (var name in AdditionalMeterNames)
                {
                    if (!string.IsNullOrWhiteSpace(name))
                        metrics.AddMeter(name);
                }
                metrics.AddPrometheusExporter();
            });

        _app = builder.Build();
        _app.UseWebSockets(new WebSocketOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(30),
        });

        UseCorsHeaders(_app);
        MapHealthEndpoints(_app);
        MapMetricsEndpoint(_app);
        MapSymbolEndpoints(_app);
        MapInstrumentEndpoint(_app);
        MapRecoveryEndpoint(_app);
        MapWebSocketEndpoint(_app, ct);

        await _app.StartAsync(ct);
        _logger.LogInformation("WebSocket server listening on port {Port}", port);
    }

    private static void UseCorsHeaders(WebApplication app)
    {
        // CORS for cross-origin frontend access. Permissive by design — this
        // service is intentionally read-only / public-by-default for the
        // dashboard. Tighten via reverse proxy if exposed externally.
        app.Use(async (context, next) =>
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
    }

    private void MapHealthEndpoints(WebApplication app)
    {
        app.MapGet("/health", () =>
        {
            var result = new HealthResponse
            {
                Status = _subscriptionManager.IsReady ? "ready" : "initializing",
                Uptime = _uptime.Elapsed.ToString(@"hh\:mm\:ss"),
            };
            if (FeedStateProvider is not null)
                result.FeedGroups = new Dictionary<string, string>(FeedStateProvider());
            if (LastPacketTimestampProvider is not null)
            {
                var timestamps = LastPacketTimestampProvider();
                var seconds = new Dictionary<string, double>(timestamps.Count);
                long now = Environment.TickCount64;
                foreach (var (k, v) in timestamps)
                    seconds[k] = v > 0 ? (now - v) / 1000.0 : -1.0;
                result.SecondsSinceLastPacket = seconds;
            }
            return Results.Json(result, AppJsonContext.Default.HealthResponse);
        });

        app.MapGet("/ready", () =>
            _subscriptionManager.IsReady ? Results.Ok("ready") : Results.StatusCode(503));

        app.MapGet("/live", () => Results.Ok("alive"));
    }

    private static void MapMetricsEndpoint(WebApplication app)
    {
        // Prometheus scrape endpoint. Exposes every meter registered in the
        // OpenTelemetry pipeline (always B3.Umdf, plus AdditionalMeterNames).
        // Lives on the same port as /health for ops-stack convenience.
        app.MapPrometheusScrapingEndpoint();
    }

    private void MapSymbolEndpoints(WebApplication app)
    {
        app.MapGet("/symbols", (string? q, int? limit) =>
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
    }

    private void MapRecoveryEndpoint(WebApplication app)
    {
        // /api/recovery/recent surfaces the in-memory ring buffer of recent
        // recovery audit events for ops triage. When no provider is wired
        // (tests, embedded scenarios) the endpoint returns an empty list
        // rather than 404 so the contract stays stable.
        app.MapGet("/api/recovery/recent", (int? limit) =>
        {
            var capped = Math.Clamp(limit ?? 50, 1, 1000);
            var events = RecoveryEventProvider?.Invoke(capped) ?? Array.Empty<RecoveryEvent>();
            var dto = new RecoveryEventLogResponse
            {
                TotalRecorded = RecoveryEventTotalProvider?.Invoke() ?? 0,
                Returned = events.Count,
                Events = new RecoveryEventDto[events.Count],
            };
            for (int i = 0; i < events.Count; i++)
            {
                var e = events[i];
                dto.Events[i] = new RecoveryEventDto
                {
                    TimestampUnixMs = e.TimestampUnixMs,
                    Kind = (int)e.Kind,
                    KindName = e.Kind.ToString(),
                    GroupId = e.GroupId,
                    SecurityId = e.SecurityId,
                    SnapshotRptSeq = e.SnapshotRptSeq,
                    PriorRptSeq = e.PriorRptSeq,
                    Detail = e.Detail,
                };
            }
            return Results.Json(dto, AppJsonContext.Default.RecoveryEventLogResponse);
        });
    }

    private void MapInstrumentEndpoint(WebApplication app)
    {
        app.MapGet("/instrument/{symbol}", (string symbol) =>
        {
            var registry = _subscriptionManager.SymbolRegistry;
            if (registry is null)
                return Results.StatusCode(503);

            symbol = symbol.Trim().ToUpperInvariant();
            if (!registry.TryResolve(symbol, out var securityId))
                return Results.NotFound();
            var info = _subscriptionManager.FindInstrumentInfo(securityId);
            if (info is null)
                return Results.NotFound();

            var resp = BuildInstrumentInfoResponse(securityId, info);
            return Results.Json(resp, AppJsonContext.Default.InstrumentInfoResponse);
        });
    }

    /// <summary>
    /// Pure mapping helper: copy every field from the internal <see cref="InstrumentInfo"/>
    /// snapshot into the public DTO. Mechanical and large by necessity (~50 fields covering
    /// reference data + statistics + collections); kept separate so the endpoint registration
    /// stays scannable.
    /// </summary>
    private static InstrumentInfoResponse BuildInstrumentInfoResponse(ulong securityId, InstrumentInfo info)
    {
        return new InstrumentInfoResponse
        {
            SecurityId = securityId,
            Symbol = info.Symbol,
            Asset = info.Asset,
            IsinNumber = info.IsinNumber,
            Currency = info.Currency,
            CfiCode = info.CfiCode,
            SecurityGroup = info.SecurityGroup,
            SecurityDescription = info.SecurityDescription,
            SecurityType = info.SecurityType,
            SecuritySubType = info.SecuritySubType,
            Product = info.Product,
            MinPriceIncrement = info.MinPriceIncrement,
            PriceDivisor = info.PriceDivisor,
            ContractMultiplier = info.ContractMultiplier,
            StrikePrice = info.StrikePrice,
            MaturityDate = info.MaturityDate,
            PutOrCall = info.PutOrCall,
            ExerciseStyle = info.ExerciseStyle,
            MarketSegmentID = info.MarketSegmentID,
            TickSizeDenominator = info.TickSizeDenominator,
            TradingStatus = info.TradingStatus,
            TradingEvent = info.TradingEvent,
            OpeningPrice = info.OpeningPrice,
            ClosingPrice = info.ClosingPrice,
            HighPrice = info.HighPrice,
            LowPrice = info.LowPrice,
            LastTradePrice = info.LastTradePrice,
            LastTradeSize = info.LastTradeSize,
            SettlementPrice = info.SettlementPrice,
            TheoreticalOpeningPrice = info.TheoreticalOpeningPrice,
            TheoreticalOpeningSize = info.TheoreticalOpeningSize,
            AuctionImbalanceSize = info.AuctionImbalanceSize,
            PriceBandLow = info.PriceBandLow,
            PriceBandHigh = info.PriceBandHigh,
            PriceLimitType = info.PriceLimitType,
            TradingReferencePrice = info.TradingReferencePrice,
            AvgDailyTradedQty = info.AvgDailyTradedQty,
            MaxTradeVol = info.MaxTradeVol,
            TradeVolume = info.TradeVolume,
            VwapPrice = info.VwapPrice,
            NetChangeFromPrevDay = info.NetChangeFromPrevDay,
            NumberOfTrades = info.NumberOfTrades,
            OpenInterest = info.OpenInterest,
            LastUpdateTimestamp = info.LastUpdateTimestamp,
            Underlyings = info.Underlyings?.Select(u => new UnderlyingResponse
            {
                SecurityId = u.SecurityId,
                Symbol = u.Symbol,
            }).ToList(),
            Legs = info.Legs?.Select(l => new LegResponse
            {
                SecurityId = l.SecurityId,
                Symbol = l.Symbol,
                RatioQty = l.RatioQty,
                SecurityType = l.SecurityType,
                Side = l.Side,
            }).ToList(),
            InstrAttribs = info.InstrAttribs?.Select(a => new InstrAttribResponse
            {
                Type = a.Type,
                Value = a.Value,
            }).ToList(),
        };
    }

    private void MapWebSocketEndpoint(WebApplication app, CancellationToken ct)
    {
        app.Map("/ws", async context =>
        {
            // Once shutdown has been signaled, refuse new connections immediately so the
            // drain phase does not have to wait on freshly-accepted clients (and so we
            // don't keep logging the connect/disconnect churn from clients that retry).
            if (_isShuttingDown || ct.IsCancellationRequested)
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
                channelCapacity: _clientChannelCapacity,
                slowClientThreshold: _slowClientThreshold,
                slowClientMaxTicks: _slowClientMaxTicks,
                maxPendingBytes: _clientMaxPendingBytes,
                coalesceWindowMs: _clientCoalesceWindowMs,
                logger: _logger);
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
                session.Cancel();   // signal write loop to stop before awaiting
                await writeTask;    // wait for clean exit before disposing CTS
                session.Dispose();
            }
        });
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

        if (closeHandshakeBudget <= TimeSpan.Zero)
            closeHandshakeBudget = TimeSpan.FromSeconds(5);

        var sessions = _subscriptionManager.EnumerateAllClients()
            .Select(kv => kv.Value)
            .ToList();
        if (sessions.Count > 0)
        {
            using var cts = new CancellationTokenSource(closeHandshakeBudget);
            var tasks = new Task[sessions.Count];
            for (int i = 0; i < sessions.Count; i++)
                tasks[i] = sessions[i].RequestGracefulCloseAsync("server shutting down", cts.Token);
            try { await Task.WhenAll(tasks).ConfigureAwait(false); }
            catch { /* per-client failures already logged inside RequestGracefulCloseAsync */ }
            _logger.LogInformation(
                "Sent WebSocket close (1001) to {Count} active client(s) before stopping Kestrel",
                sessions.Count);
        }

        await _app.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _shutdownRegistration.Dispose();
        if (_app is not null)
            await _app.DisposeAsync();
    }
}
