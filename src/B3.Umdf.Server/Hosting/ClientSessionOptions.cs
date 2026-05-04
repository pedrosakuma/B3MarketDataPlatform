namespace B3.Umdf.Server.Hosting;

/// <summary>
/// Per-WebSocket-connection tuning surface consumed by
/// <see cref="WebSocketConnectionHandler"/> when constructing a
/// <see cref="ClientSession"/>. Grouped so the WebSocketHost ctor doesn't
/// need to re-thread six independent primitives every time the surface grows.
/// </summary>
public sealed record ClientSessionOptions(
    int ChannelCapacity = 4096,
    double SlowClientThreshold = 0.75,
    int SlowClientMaxTicks = 100,
    long MaxPendingBytes = 16L * 1024 * 1024,
    int CoalesceWindowMs = 0);
