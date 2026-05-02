using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Verifies that <see cref="WebSocketHost"/> exposes a Prometheus-format
/// <c>/metrics</c> endpoint on the same port as the health endpoints, and
/// that it surfaces the meters registered via the OTel pipeline (the
/// always-on <c>"B3.Umdf"</c> meter plus any meter named in
/// <see cref="WebSocketHost.AdditionalMeterNames"/>).
/// </summary>
public class PrometheusMetricsEndpointTests
{
    private static int FindFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    [Fact]
    public async Task Metrics_Endpoint_ServesPrometheusExposition()
    {
        var sm = new SubscriptionManager();
        var host = new WebSocketHost(sm);
        var port = FindFreePort();
        var cts = new CancellationTokenSource();

        await host.StartAsync(port, cts.Token);
        try
        {
            // Increment AFTER the OTel MeterProvider is built so the SDK
            // observes a non-zero delta on the first scrape.
            MetricsRegistry.WsMessagesSent.Add(1);

            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var resp = await http.GetAsync($"http://127.0.0.1:{port}/metrics");

            Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

            var contentType = resp.Content.Headers.ContentType?.MediaType;
            // Prometheus exposition format is text/plain (the version
            // suffix lives in the parameters and is allowed to vary).
            Assert.Equal("text/plain", contentType);

            var body = await resp.Content.ReadAsStringAsync();

            // Prometheus exposition uses '_' separators, so dotted meter
            // names like "umdf.ws.messages.sent" become "umdf_ws_messages_sent".
            Assert.Contains("umdf_ws_messages_sent", body);
            // The active-symbols ObservableGauge wired in MetricsRegistry's
            // static ctor should always emit (returns 0 when the provider
            // is null, which is the case in this isolated test).
            Assert.Contains("umdf_ws_subscribed_symbols", body);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task Metrics_Endpoint_IncludesAdditionalMeterNames()
    {
        var sm = new SubscriptionManager();
        var host = new WebSocketHost(sm)
        {
            AdditionalMeterNames = new[] { "B3.Umdf.Server.Tests.Extra" },
        };

        // Create a counter on the additional meter BEFORE StartAsync so
        // the OTel SDK picks it up when the MeterProvider is built.
        using var extraMeter = new System.Diagnostics.Metrics.Meter("B3.Umdf.Server.Tests.Extra", "1.0.0");
        var extraCounter = extraMeter.CreateCounter<long>("test.extra.counter");

        var port = FindFreePort();
        var cts = new CancellationTokenSource();
        await host.StartAsync(port, cts.Token);
        try
        {
            extraCounter.Add(42);

            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var resp = await http.GetAsync($"http://127.0.0.1:{port}/metrics");
            Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

            var body = await resp.Content.ReadAsStringAsync();
            Assert.Contains("test_extra_counter", body);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }
}
