using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;

namespace B3.Umdf.Server.Hosting;

/// <summary>
/// Owns the OpenTelemetry meter registration plus the Prometheus scrape
/// endpoint mapping. Always exposes the <c>"B3.Umdf"</c> meter (defined by
/// <see cref="MetricsRegistry"/>); callers may pass extra meter names to
/// surface (e.g. <c>"B3.Umdf.Consumer"</c> from the ConsoleApp's
/// <c>MetricsBinder</c>) on the same scrape endpoint.
/// </summary>
internal static class MetricsEndpointMapper
{
    public static void AddOpenTelemetryWithMeters(IServiceCollection services, IReadOnlyList<string> additionalMeterNames)
    {
        services
            .AddOpenTelemetry()
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(MetricsRegistry.Meter.Name);
                foreach (var name in additionalMeterNames)
                {
                    if (!string.IsNullOrWhiteSpace(name))
                        metrics.AddMeter(name);
                }
                metrics.AddPrometheusExporter();
            });
    }

    public static void MapPrometheus(WebApplication app)
    {
        // Prometheus scrape endpoint. Lives on the same port as /health for
        // ops-stack convenience.
        app.MapPrometheusScrapingEndpoint();
    }
}
