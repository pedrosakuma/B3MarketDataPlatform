namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Bitmask matching the server's <c>DataFlags</c> enum, scoped to the
/// channels exposed by v1 of the SDK. Combine with <c>|</c>:
/// <c>Trades | Info</c>.
/// </summary>
[Flags]
public enum SubscribeFlags : byte
{
    /// <summary>
    /// <c>InfoSnapshot</c> + incremental info updates (carries
    /// <c>LastTradePrice</c>, <c>LastTradeSize</c>, status, etc.).
    /// </summary>
    Info = 0x02,

    /// <summary>
    /// Live trade prints and corrections (<c>TradeBust</c>), plus the
    /// per-symbol recent-trades history snapshot on subscribe.
    /// </summary>
    Trades = 0x10,
}
