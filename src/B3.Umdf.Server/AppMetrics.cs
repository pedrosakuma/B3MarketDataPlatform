using System.Diagnostics.Metrics;

namespace B3.Umdf.Server;

/// <summary>
/// OpenTelemetry-compatible metrics using System.Diagnostics.Metrics.
/// Meter name: "B3.Umdf" — collect via OTLP or Prometheus scrape.
/// </summary>
public sealed class AppMetrics
{
    public static readonly Meter Meter = new("B3.Umdf", "1.0.0");

    // Feed metrics
    public static readonly Counter<long> PacketsReceived = Meter.CreateCounter<long>("umdf.packets.received");
    public static readonly Counter<long> GapsDetected = Meter.CreateCounter<long>("umdf.gaps.detected");
    public static readonly Counter<long> ParseErrors = Meter.CreateCounter<long>("umdf.parse_errors");

    // Book metrics
    public static readonly Counter<long> OrdersProcessed = Meter.CreateCounter<long>("umdf.orders.processed");
    public static readonly Counter<long> TradesProcessed = Meter.CreateCounter<long>("umdf.trades.processed");
    public static readonly Counter<long> DeletesProcessed = Meter.CreateCounter<long>("umdf.deletes.processed");

    // WebSocket metrics
    public static readonly UpDownCounter<int> WsConnectionsActive = Meter.CreateUpDownCounter<int>("umdf.ws.connections.active");
    public static readonly Counter<long> WsMessagesSent = Meter.CreateCounter<long>("umdf.ws.messages.sent");
    public static readonly Counter<long> WsMessagesDropped = Meter.CreateCounter<long>("umdf.ws.messages.dropped");
    public static readonly Counter<long> WsSlowDisconnects = Meter.CreateCounter<long>("umdf.ws.slow_disconnects");
    public static readonly Counter<long> WsSubscriptions = Meter.CreateCounter<long>("umdf.ws.subscriptions");
}
