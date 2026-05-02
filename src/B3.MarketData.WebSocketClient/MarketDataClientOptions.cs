namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Strategy for handling a slow consumer when the SDK's internal event
/// channel can't be drained fast enough by the application.
/// </summary>
public enum BackPressurePolicy
{
    /// <summary>
    /// Drop the oldest pending event to make room for the new one.
    /// Increments <see cref="MarketDataClient.DroppedEventCount"/>.
    /// Default — safest for "live last-value" consumers (e.g. reference
    /// price), where stale events behind the head are useless anyway.
    /// </summary>
    DropOldest = 0,

    /// <summary>
    /// Block the receive loop until there is room. Will eventually
    /// trigger server-side slow-consumer disconnect (1008) if the
    /// application stays stalled, since the receive loop also drains
    /// the underlying TCP buffers.
    /// </summary>
    Block = 1,

    /// <summary>
    /// Throw an <see cref="InvalidOperationException"/> from the receive
    /// loop, surfaced via <see cref="MarketDataClient.ConnectionStateChanged"/>
    /// and forcing reconnect. Useful for fail-fast deployments.
    /// </summary>
    Throw = 2,
}

/// <summary>
/// Configuration for <see cref="MarketDataClient"/>. All values have
/// defaults tuned for the reference-price use case.
/// </summary>
public sealed class MarketDataClientOptions
{
    /// <summary>
    /// WebSocket endpoint. Required. Use <c>ws://</c> for plaintext or
    /// <c>wss://</c> when terminating TLS at an ingress.
    /// </summary>
    public Uri Endpoint { get; set; } = new("ws://localhost:8080/ws");

    /// <summary>
    /// Bound for the internal event channel. Default 4096 — sized for
    /// burst tolerance of one second at peak trade rate.
    /// </summary>
    public int EventChannelCapacity { get; set; } = 4096;

    /// <summary>
    /// Policy applied when <see cref="EventChannelCapacity"/> is full.
    /// Default <see cref="BackPressurePolicy.DropOldest"/>.
    /// </summary>
    public BackPressurePolicy BackPressure { get; set; } = BackPressurePolicy.DropOldest;

    /// <summary>
    /// Initial delay before the first reconnect attempt. Doubles each
    /// failed attempt up to <see cref="ReconnectMaxDelay"/>, with up to
    /// 25% additive jitter. Default 250 ms.
    /// </summary>
    public TimeSpan ReconnectInitialDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Cap on the exponential reconnect back-off. Default 30 s.
    /// </summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// When <c>true</c> (default), the SDK transparently re-issues every
    /// active subscription after a reconnect — the application observes
    /// only a <c>ConnectionStateChanged</c> event, no resubscribe loop
    /// of its own.
    /// </summary>
    public bool AutoResubscribeOnReconnect { get; set; } = true;
}
