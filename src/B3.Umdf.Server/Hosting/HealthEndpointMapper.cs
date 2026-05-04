using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace B3.Umdf.Server.Hosting;

/// <summary>
/// Maps the orchestration-facing probes (<c>/health</c>, <c>/ready</c>,
/// <c>/live</c>). Carries no logic of its own; the staleness gate lives in
/// <see cref="HealthEvaluator"/> so it can be unit-tested in isolation.
/// </summary>
internal sealed class HealthEndpointMapper
{
    private readonly SubscriptionManager _subscriptionManager;
    private readonly Stopwatch _uptime;
    private readonly Func<int> _maxStaleSeconds;
    private readonly Func<bool> _failOnRecovery;
    private readonly Func<Func<IReadOnlyDictionary<string, string>>?> _feedStateProvider;
    private readonly Func<Func<IReadOnlyDictionary<string, long>>?> _lastPacketProvider;

    public HealthEndpointMapper(
        SubscriptionManager subscriptionManager,
        Stopwatch uptime,
        Func<int> maxStaleSeconds,
        Func<bool> failOnRecovery,
        Func<Func<IReadOnlyDictionary<string, string>>?> feedStateProvider,
        Func<Func<IReadOnlyDictionary<string, long>>?> lastPacketProvider)
    {
        _subscriptionManager = subscriptionManager;
        _uptime = uptime;
        _maxStaleSeconds = maxStaleSeconds;
        _failOnRecovery = failOnRecovery;
        _feedStateProvider = feedStateProvider;
        _lastPacketProvider = lastPacketProvider;
    }

    public void Map(WebApplication app)
    {
        app.MapGet("/health", () =>
        {
            var result = new HealthResponse
            {
                Status = _subscriptionManager.IsReady ? "ready" : "initializing",
                Uptime = _uptime.Elapsed.ToString(@"hh\:mm\:ss"),
            };

            IReadOnlyDictionary<string, string>? states = null;
            var stateProvider = _feedStateProvider();
            if (stateProvider is not null)
            {
                states = stateProvider();
                result.FeedGroups = new Dictionary<string, string>(states);
            }

            IReadOnlyDictionary<string, long>? lastPackets = null;
            var lpProvider = _lastPacketProvider();
            if (lpProvider is not null)
            {
                lastPackets = lpProvider();
                var seconds = new Dictionary<string, double>(lastPackets.Count);
                long now = Environment.TickCount64;
                foreach (var (k, v) in lastPackets)
                    seconds[k] = v > 0 ? (now - v) / 1000.0 : -1.0;
                result.SecondsSinceLastPacket = seconds;
            }

            var unhealthy = HealthEvaluator.FindUnhealthyGroups(
                states,
                lastPackets,
                Environment.TickCount64,
                _uptime.Elapsed.TotalSeconds,
                _maxStaleSeconds(),
                _failOnRecovery());

            if (unhealthy is not null)
            {
                result.Reason = "Stale recovery: " + string.Join(", ", unhealthy);
                return Results.Json(result, AppJsonContext.Default.HealthResponse, statusCode: 503);
            }

            return Results.Json(result, AppJsonContext.Default.HealthResponse);
        });

        app.MapGet("/ready", () =>
            _subscriptionManager.IsReady ? Results.Ok("ready") : Results.StatusCode(503));

        app.MapGet("/live", () => Results.Ok("alive"));
    }
}
