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

    private SubscriptionRequest(SubscriptionRequestKind kind, string clientId, string? symbol, ulong securityId, DataFlags flags)
    {
        Kind = kind;
        ClientId = clientId;
        Symbol = symbol;
        SecurityId = securityId;
        Flags = flags;
    }

    public static SubscriptionRequest Subscribe(string clientId, string symbol, DataFlags flags)
        => new(SubscriptionRequestKind.Subscribe, clientId, symbol, 0, flags);

    public static SubscriptionRequest Get(string clientId, string symbol, DataFlags flags)
        => new(SubscriptionRequestKind.Get, clientId, symbol, 0, flags);

    public static SubscriptionRequest Unsubscribe(string clientId, ulong securityId)
        => new(SubscriptionRequestKind.Unsubscribe, clientId, null, securityId, DataFlags.None);

    public static SubscriptionRequest UnsubscribeAll(string clientId)
        => new(SubscriptionRequestKind.UnsubscribeAll, clientId, null, 0, DataFlags.None);
}
