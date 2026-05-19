namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Bitmask matching the server's <c>DataFlags</c> enum. Combine with <c>|</c>
/// (e.g. <c>Trades | Info | Mbp</c>). Bit positions are part of the wire
/// contract — never reorder or repurpose.
/// </summary>
[Flags]
public enum SubscribeFlags : byte
{
    /// <summary>
    /// L3 / order-by-order book: <see cref="MarketDataClient.BookSnapshot"/> on
    /// subscribe + incremental <see cref="MarketDataClient.OrderAdded"/> /
    /// <see cref="MarketDataClient.OrderUpdated"/> /
    /// <see cref="MarketDataClient.OrderDeleted"/> /
    /// <see cref="MarketDataClient.BookCleared"/> /
    /// <see cref="MarketDataClient.MarketTierUpdate"/>.
    /// </summary>
    Book = 0x01,

    /// <summary>
    /// <c>InfoSnapshot</c> + incremental info updates (carries
    /// <c>LastTradePrice</c>, <c>LastTradeSize</c>, status, etc.).
    /// </summary>
    Info = 0x02,

    /// <summary>
    /// News deliveries (<see cref="MarketDataClient.News"/>). Opt-in: clients
    /// without this bit receive no news. Both instrument-scoped and global
    /// news flow through this flag.
    /// </summary>
    News = 0x04,

    /// <summary>
    /// Aggregated price-level (MBP) stream:
    /// <see cref="MarketDataClient.LevelSnapshot"/> on subscribe +
    /// incremental <see cref="MarketDataClient.LevelUpdate"/> /
    /// <see cref="MarketDataClient.LevelDeleted"/>. Independent of
    /// <see cref="Book"/>: a client may request only MBP, only MBO, or both.
    /// Recommended default for UI consumers (conflated per
    /// <c>(secId, side, price)</c>, so much less bandwidth on hot levels).
    /// </summary>
    Mbp = 0x08,

    /// <summary>
    /// Live trade prints and corrections (<c>TradeBust</c>), plus the
    /// per-symbol recent-trades history snapshot on subscribe.
    /// </summary>
    Trades = 0x10,

    /// <summary>Legacy convenience: <see cref="Book"/> + <see cref="Info"/>.
    /// Mirrors the server's <c>DataFlags.All</c> — does NOT include News, MBP, or Trades.</summary>
    All = Book | Info,

    /// <summary>Convenience alias for every data class: Book + Info + News + MBP + Trades.</summary>
    Everything = Book | Info | News | Mbp | Trades,
}
