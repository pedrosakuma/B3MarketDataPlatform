namespace B3.MarketData.WebSocketClient;

/// <summary>Options for one symbol subscription.</summary>
public sealed class SubscriptionOptions
{
    /// <summary>Channels requested for the symbol.</summary>
    public SubscribeFlags Flags { get; init; } = SubscribeFlags.Trades;

    /// <summary>
    /// Requested fixed cadence for <see cref="SubscribeFlags.ConflatedMbp"/>.
    /// The server accepts only its configured safe set (100/250/500 ms by
    /// default). Leave null for ordinary per-packet channels.
    /// </summary>
    public TimeSpan? ConflationInterval { get; init; }
}
