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

    /// <summary>
    /// Static per-security metadata (tick size / lot size and full
    /// <c>SecurityDefinition_12</c> projection) via
    /// <see cref="MarketDataClient.SecurityDefinition"/>. Bootstrap snapshot
    /// on subscribe + push on every real definition change. Opt-in so legacy
    /// clients keep their current bandwidth profile; required by pre-trade
    /// guards that need <c>MinPriceIncrement</c> / <c>MinTradeVol</c> without
    /// a REST refetch.
    /// </summary>
    SecurityDefinition = 0x20,

    /// <summary>
    /// Dynamic per-symbol price band ("túnel de preço") via
    /// <see cref="MarketDataClient.PriceBand"/>: low/high limits, limit-type,
    /// midpoint-type, and trading reference price from <c>PriceBand_22</c>.
    /// Bootstrap snapshot on subscribe (when the server has already observed
    /// the band) + push on every real band change. Opt-in so legacy clients
    /// keep their current bandwidth profile; required by pre-trade
    /// fat-finger guards that want the venue-authoritative band instead of
    /// a static config.
    /// </summary>
    PriceBand = 0x40,

    /// <summary>
    /// Aggregated auction state: imbalance qty/side from <c>AuctionImbalance_19</c>
    /// plus trading phase from <c>SecurityGroupPhase_10</c>.
    /// Bootstrap snapshot on subscribe + push on every real delta.
    /// Opt-in for clients that need to display pre-open / auction-phase
    /// state or build imbalance-aware algo logic.
    /// </summary>
    Auction = 0x80,

    /// <summary>Legacy convenience: <see cref="Book"/> + <see cref="Info"/>.
    /// Mirrors the server's <c>DataFlags.All</c> — does NOT include News, MBP, Trades, SecurityDefinition, PriceBand, or Auction.</summary>
    All = Book | Info,

    /// <summary>Convenience alias for every data class: Book + Info + News + MBP + Trades + SecurityDefinition + PriceBand + Auction.</summary>
    Everything = Book | Info | News | Mbp | Trades | SecurityDefinition | PriceBand | Auction,
}
