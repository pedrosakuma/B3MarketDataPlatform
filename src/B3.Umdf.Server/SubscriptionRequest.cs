namespace B3.Umdf.Server;

internal enum SubscriptionRequestKind : byte
{
    Subscribe,
    Get,
    Unsubscribe,
    UnsubscribeAll,
}

internal readonly struct SubscriptionRequest
{
    public SubscriptionRequestKind Kind { get; }
    public string ClientId { get; }
    public string? Symbol { get; }
    public ulong SecurityId { get; }
    public DataFlags Flags { get; }
    public ushort ConflationIntervalMs { get; }

    private SubscriptionRequest(
        SubscriptionRequestKind kind,
        string clientId,
        string? symbol,
        ulong securityId,
        DataFlags flags,
        ushort conflationIntervalMs)
    {
        Kind = kind;
        ClientId = clientId;
        Symbol = symbol;
        SecurityId = securityId;
        Flags = flags;
        ConflationIntervalMs = conflationIntervalMs;
    }

    public static SubscriptionRequest Subscribe(
        string clientId,
        string symbol,
        DataFlags flags,
        ushort conflationIntervalMs = 0)
        => new(SubscriptionRequestKind.Subscribe, clientId, symbol, 0, flags, conflationIntervalMs);

    public static SubscriptionRequest Get(
        string clientId,
        string symbol,
        DataFlags flags,
        ushort conflationIntervalMs = 0)
        => new(SubscriptionRequestKind.Get, clientId, symbol, 0, flags, conflationIntervalMs);

    public static SubscriptionRequest Unsubscribe(string clientId, ulong securityId)
        => new(SubscriptionRequestKind.Unsubscribe, clientId, null, securityId, DataFlags.None, 0);

    public static SubscriptionRequest UnsubscribeAll(string clientId)
        => new(SubscriptionRequestKind.UnsubscribeAll, clientId, null, 0, DataFlags.None, 0);
}
