using System.Diagnostics.Metrics;

namespace B3.Umdf.Server;

/// <summary>
/// OpenTelemetry-compatible metrics using System.Diagnostics.Metrics.
/// Meter name: "B3.Umdf" — collect via OTLP or Prometheus scrape.
/// </summary>
public sealed class MetricsRegistry
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
    public static readonly Counter<long> WsSlowDisconnects = Meter.CreateCounter<long>("umdf.ws.slow_disconnects");
    public static readonly Counter<long> WsSubscriptions = Meter.CreateCounter<long>("umdf.ws.subscriptions");
    public static readonly Counter<long> WsMessagesConflated = Meter.CreateCounter<long>("umdf.ws.messages.conflated");

    // BroadcastBufferPool metrics — retention/efficiency of the pool that
    // backs the broadcast → write-loop ownership transfer (replaces
    // ArrayPool<byte>.Shared on that path to avoid per-bucket Monitor
    // contention from cross-thread Returns).
    public static readonly Counter<long> BroadcastBufferRentHits = Meter.CreateCounter<long>("umdf.broadcast_pool.rent_hits");
    public static readonly Counter<long> BroadcastBufferRentMisses = Meter.CreateCounter<long>("umdf.broadcast_pool.rent_misses");
    public static readonly Counter<long> BroadcastBufferReturnDrops = Meter.CreateCounter<long>("umdf.broadcast_pool.return_drops");
    public static readonly Counter<long> BroadcastBufferOversizeRents = Meter.CreateCounter<long>("umdf.broadcast_pool.oversize_rents");

    // ── Observable gauges (read at scrape time only — zero hot-path cost) ──
    //
    // These are lazily wired from a callback so the Server library has no
    // direct dependency on SubscriptionManager construction order. Set the
    // provider before starting the host; null (default) disables emission.

    /// <summary>
    /// Provider for <c>umdf.ws.subscribed_symbols</c> — distinct securities with
    /// at least one active subscriber. Set once at startup.
    /// </summary>
    public static Func<int>? ActiveSubscribedSymbolsProvider { get; set; }

    static MetricsRegistry()
    {
        Meter.CreateObservableGauge(
            "umdf.ws.subscribed_symbols",
            static () => ActiveSubscribedSymbolsProvider?.Invoke() ?? 0,
            description: "Distinct securities with at least one active WebSocket subscriber.");
    }
}
