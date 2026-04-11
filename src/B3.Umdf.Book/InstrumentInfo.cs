namespace B3.Umdf.Book;

/// <summary>
/// Per-instrument market data beyond the order book (prices, stats, status).
/// All prices are raw SBE mantissa values (multiply by 10^-exponent to get decimal).
/// Enum-like fields use int? to avoid coupling with generated SBE enums.
/// </summary>
public sealed class InstrumentInfo
{
    // SecurityStatus (3)
    public int? TradingStatus { get; set; }
    public int? TradingEvent { get; set; }
    public ulong? TradSesOpenTime { get; set; }

    // Prices
    public long? OpeningPrice { get; set; }
    public long? ClosingPrice { get; set; }
    public long? HighPrice { get; set; }
    public long? LowPrice { get; set; }
    public long? LastTradePrice { get; set; }
    public long? LastTradeSize { get; set; }
    public long? SettlementPrice { get; set; }
    public long? TheoreticalOpeningPrice { get; set; }
    public long? TheoreticalOpeningSize { get; set; }

    // Auction
    public long? AuctionImbalanceSize { get; set; }

    // Bands
    public long? PriceBandLow { get; set; }
    public long? PriceBandHigh { get; set; }
    public long? TradingReferencePrice { get; set; }
    public long? AvgDailyTradedQty { get; set; }
    public long? MaxTradeVol { get; set; }

    // Statistics
    public long? TradeVolume { get; set; }
    public long? VwapPrice { get; set; }
    public long? NetChangeFromPrevDay { get; set; }
    public long? NumberOfTrades { get; set; }
    public long? OpenInterest { get; set; }

    // Timestamps
    public ulong LastUpdateTimestamp { get; set; }
}
