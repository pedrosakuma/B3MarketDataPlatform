namespace B3.Umdf.Book;

/// <summary>
/// Per-instrument market data beyond the order book (prices, stats, status).
/// All prices are raw SBE mantissa values (multiply by 10^-exponent to get decimal).
/// Enum-like fields use int? to avoid coupling with generated SBE enums.
/// </summary>
public sealed class InstrumentInfo
{
    private long _version;

    /// <summary>Monotonic version counter. Bumped by the feed thread on every mutation.</summary>
    public long Version => Volatile.Read(ref _version);

    /// <summary>Increment the version counter after updating fields. Feed-thread-only.</summary>
    public void BumpVersion() => ++_version;

    // SecurityStatus (3)
    public int? TradingStatus { get; set; }
    public int? TradingEvent { get; set; }
    public ulong? TradSesOpenTime { get; set; }

    // SecurityDefinition / group tracking
    public string? SecurityGroup { get; set; }
    /// <summary>When true, this instrument's TradingStatus follows its SecurityGroup phase.</summary>
    public bool FollowsGroupStatus { get; set; } = true;

    // Static instrument metadata (from SecurityDefinition_12)
    public string? Symbol { get; set; }
    public string? Asset { get; set; }
    public string? IsinNumber { get; set; }
    public string? Currency { get; set; }
    public string? CfiCode { get; set; }
    public int? SecurityType { get; set; }
    public int? SecuritySubType { get; set; }
    public int? Product { get; set; }
    public long? MinPriceIncrement { get; set; }
    public long? PriceDivisor { get; set; }
    public long? ContractMultiplier { get; set; }
    public long? StrikePrice { get; set; }
    public int? MaturityDate { get; set; }
    public int? PutOrCall { get; set; }
    public int? ExerciseStyle { get; set; }
    public int? MarketSegmentID { get; set; }
    public int? TickSizeDenominator { get; set; }

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

    // SecurityDefinition repeating groups
    public string? SecurityDescription { get; set; }
    public List<UnderlyingInfo>? Underlyings { get; set; }
    public List<LegInfo>? Legs { get; set; }
    public List<InstrAttribInfo>? InstrAttribs { get; set; }

    /// <summary>Reset all fields to null/default.</summary>
    public void Reset()
    {
        TradingStatus = null;
        TradingEvent = null;
        TradSesOpenTime = null;
        SecurityGroup = null;
        FollowsGroupStatus = true;
        Symbol = null;
        Asset = null;
        IsinNumber = null;
        Currency = null;
        CfiCode = null;
        SecurityType = null;
        SecuritySubType = null;
        Product = null;
        MinPriceIncrement = null;
        PriceDivisor = null;
        ContractMultiplier = null;
        StrikePrice = null;
        MaturityDate = null;
        PutOrCall = null;
        ExerciseStyle = null;
        MarketSegmentID = null;
        TickSizeDenominator = null;
        OpeningPrice = null;
        ClosingPrice = null;
        HighPrice = null;
        LowPrice = null;
        LastTradePrice = null;
        LastTradeSize = null;
        SettlementPrice = null;
        TheoreticalOpeningPrice = null;
        TheoreticalOpeningSize = null;
        AuctionImbalanceSize = null;
        PriceBandLow = null;
        PriceBandHigh = null;
        TradingReferencePrice = null;
        AvgDailyTradedQty = null;
        MaxTradeVol = null;
        TradeVolume = null;
        VwapPrice = null;
        NetChangeFromPrevDay = null;
        NumberOfTrades = null;
        OpenInterest = null;
        LastUpdateTimestamp = 0;
        SecurityDescription = null;
        Underlyings = null;
        Legs = null;
        InstrAttribs = null;
        BumpVersion();
    }
}

public sealed class UnderlyingInfo
{
    public ulong SecurityId { get; set; }
    public string? Symbol { get; set; }
}

public sealed class LegInfo
{
    public ulong SecurityId { get; set; }
    public string? Symbol { get; set; }
    public long? RatioQty { get; set; }
    public int? SecurityType { get; set; }
    public int? Side { get; set; }
}

public sealed class InstrAttribInfo
{
    public int Type { get; set; }
    public int Value { get; set; }
}
