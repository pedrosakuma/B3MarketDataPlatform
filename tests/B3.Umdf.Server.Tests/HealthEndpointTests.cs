using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text.Json;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Verifies the <c>/health</c> stale-recovery → 503 contract added for
/// orchestrator-driven readiness gating (issue #16). The endpoint returns
/// 200 while feed groups are Streaming or while still bootstrapping within
/// the configured threshold, and 503 (with a populated <c>Reason</c>)
/// once a non-Streaming group exceeds the threshold.
/// </summary>
public class HealthEndpointTests
{
    private static int FindFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    private sealed class HostHandle : IAsyncDisposable
    {
        public required WebSocketHost Host { get; init; }
        public required SubscriptionManager Sm { get; init; }
        public required CancellationTokenSource Cts { get; init; }
        public required int Port { get; init; }

        public async ValueTask DisposeAsync()
        {
            Cts.Cancel();
            await Host.StopAsync();
            await Host.DisposeAsync();
            Sm.Dispose();
        }
    }

    private static async Task<HostHandle> StartAsync(
        Dictionary<string, string> states,
        Dictionary<string, long> lastPacketTicks,
        int maxStaleSeconds = 60,
        bool failOnRecovery = true)
    {
        var sm = new SubscriptionManager();
        var host = new WebSocketHost(sm)
        {
            HealthMaxStaleSeconds = maxStaleSeconds,
            HealthFailOnRecovery = failOnRecovery,
            FeedStateProvider = () => states,
            LastPacketTimestampProvider = () => lastPacketTicks,
        };
        var port = FindFreePort();
        var cts = new CancellationTokenSource();
        await host.StartAsync(port, cts.Token);
        return new HostHandle { Host = host, Sm = sm, Cts = cts, Port = port };
    }

    private static async Task<(HttpStatusCode status, JsonElement body)> GetHealthAsync(int port)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var resp = await http.GetAsync($"http://127.0.0.1:{port}/health");
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        return (resp.StatusCode, doc.RootElement.Clone());
    }

    [Fact]
    public async Task Health_AllGroupsStreaming_Returns200_NoReason()
    {
        var states = new Dictionary<string, string> { ["G0"] = "Streaming", ["G1"] = "Streaming" };
        var ticks = new Dictionary<string, long> { ["G0"] = Environment.TickCount64, ["G1"] = Environment.TickCount64 };
        await using var h = await StartAsync(states, ticks);

        var (status, body) = await GetHealthAsync(h.Port);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.False(body.TryGetProperty("reason", out var reason) && reason.ValueKind == JsonValueKind.String,
            "reason field must be absent on healthy responses");
    }

    [Fact]
    public async Task Health_NonStreamingWithinThreshold_Returns200()
    {
        // Group G0 is non-Streaming but received a packet just now → staleness ≈ 0s,
        // well below the 60s default.
        var states = new Dictionary<string, string> { ["G0"] = "WaitInstrumentDefinition" };
        var ticks = new Dictionary<string, long> { ["G0"] = Environment.TickCount64 };
        await using var h = await StartAsync(states, ticks);

        var (status, _) = await GetHealthAsync(h.Port);
        Assert.Equal(HttpStatusCode.OK, status);
    }

    [Fact]
    public async Task Health_NonStreamingBeyondThreshold_Returns503_WithReason()
    {
        // Last packet 10s ago, threshold 1s → unhealthy.
        var states = new Dictionary<string, string> { ["G0"] = "WaitInstrumentDefinition" };
        var ticks = new Dictionary<string, long> { ["G0"] = Environment.TickCount64 - 10_000 };
        await using var h = await StartAsync(states, ticks, maxStaleSeconds: 1);

        var (status, body) = await GetHealthAsync(h.Port);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);
        Assert.True(body.TryGetProperty("reason", out var reason));
        Assert.Equal(JsonValueKind.String, reason.ValueKind);
        Assert.Contains("G0", reason.GetString());
    }

    [Fact]
    public async Task Health_FailOnRecoveryDisabled_Returns200_EvenWhenStale()
    {
        var states = new Dictionary<string, string> { ["G0"] = "WaitInstrumentDefinition" };
        var ticks = new Dictionary<string, long> { ["G0"] = Environment.TickCount64 - 10_000 };
        await using var h = await StartAsync(states, ticks, maxStaleSeconds: 1, failOnRecovery: false);

        var (status, body) = await GetHealthAsync(h.Port);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.False(body.TryGetProperty("reason", out var reason) && reason.ValueKind == JsonValueKind.String);
    }

    [Fact]
    public async Task Health_ColdStart_NoSnapshotYet_Returns200_NotUnhealthy()
    {
        // No packet seen yet (ticks=0). Process uptime is sub-second on a freshly
        // started host, well under the 60s default → must NOT report 503 just
        // because the group hasn't reached Streaming yet.
        var states = new Dictionary<string, string> { ["G0"] = "WaitInstrumentDefinition" };
        var ticks = new Dictionary<string, long> { ["G0"] = 0 };
        await using var h = await StartAsync(states, ticks, maxStaleSeconds: 60);

        var (status, body) = await GetHealthAsync(h.Port);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("initializing", body.GetProperty("status").GetString());
    }
}
