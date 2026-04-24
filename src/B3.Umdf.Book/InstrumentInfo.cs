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
    /// <summary>
    /// SBE PriceLimitType (tag 1306): 0=PRICE_UNIT (limits are absolute prices),
    /// 1=TICKS (limits are tick offsets vs. TradingReferencePrice combined with
    /// MinPriceIncrement), 2=PERCENTAGE (limits are % offsets vs. TradingReferencePrice).
    /// Required to interpret <see cref="PriceBandLow"/> / <see cref="PriceBandHigh"/>
    /// correctly — without it, displays show raw mantissa as if absolute price.
    /// </summary>
    public byte? PriceLimitType { get; set; }
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

    // Per-symbol, per-message-kind rptSeq tracking (Phase 0 — shadow only).
    // 0 means "no message of this kind seen yet"; gap detection should skip
    // the first observation. Each statistic message has its own independent
    // rptSeq counter per security in the B3 SBE schema.
    public uint LastRptSeqOpeningPrice;            // tpl 15
    public uint LastRptSeqTheoreticalOpeningPrice; // tpl 16
    public uint LastRptSeqClosingPrice;            // tpl 17
    public uint LastRptSeqAuctionImbalance;        // tpl 19
    public uint LastRptSeqQuantityBand;            // tpl 21
    public uint LastRptSeqPriceBand;               // tpl 22
    public uint LastRptSeqHighPrice;               // tpl 24
    public uint LastRptSeqLowPrice;                // tpl 25
    public uint LastRptSeqLastTradePrice;          // tpl 27
    public uint LastRptSeqSettlementPrice;         // tpl 28
    public uint LastRptSeqOpenInterest;            // tpl 29
    public uint LastRptSeqExecutionStatistics;     // tpl 56
    public uint LastRptSeqSecurityStatus;          // tpl 3

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
        PriceLimitType = null;
        TradingReferencePrice = null;
        AvgDailyTradedQty = null;
        MaxTradeVol = null;
        TradeVolume = null;
        VwapPrice = null;
        NetChangeFromPrevDay = null;
        NumberOfTrades = null;
        OpenInterest = null;
        LastUpdateTimestamp = 0;
        LastRptSeqOpeningPrice = 0;
        LastRptSeqTheoreticalOpeningPrice = 0;
        LastRptSeqClosingPrice = 0;
        LastRptSeqAuctionImbalance = 0;
        LastRptSeqQuantityBand = 0;
        LastRptSeqPriceBand = 0;
        LastRptSeqHighPrice = 0;
        LastRptSeqLowPrice = 0;
        LastRptSeqLastTradePrice = 0;
        LastRptSeqSettlementPrice = 0;
        LastRptSeqOpenInterest = 0;
        LastRptSeqExecutionStatistics = 0;
        LastRptSeqSecurityStatus = 0;
        SecurityDescription = null;
        Underlyings = null;
        Legs = null;
        InstrAttribs = null;
        BumpVersion();
    }

    /// <summary>
    /// Per B3 spec §14.3 — TRADING_SESSION_CHANGE (SecurityTradingEvent=4):
    /// "end of day trading statistics reset". Clears LastTradePrice,
    /// ExecutionStatistics (VWAP, TradeVolume, NumberOfTrades),
    /// OpeningPrice, TheoreticalOpeningPrice, HighPrice, LowPrice and the
    /// related rptSeq watermarks so the first post-reset stat at rptSeq=1
    /// is accepted by <see cref="SymbolStateRegistry"/>'s gap routing.
    /// Identity, instrument metadata, status and book state are preserved.
    /// </summary>
    public void ResetSessionStatistics()
    {
        LastTradePrice = null;
        LastTradeSize = null;
        OpeningPrice = null;
        TheoreticalOpeningPrice = null;
        TheoreticalOpeningSize = null;
        ClosingPrice = null;
        HighPrice = null;
        LowPrice = null;
        TradeVolume = null;
        VwapPrice = null;
        NumberOfTrades = null;
        NetChangeFromPrevDay = null;
        AuctionImbalanceSize = null;

        LastRptSeqOpeningPrice = 0;
        LastRptSeqTheoreticalOpeningPrice = 0;
        LastRptSeqClosingPrice = 0;
        LastRptSeqAuctionImbalance = 0;
        LastRptSeqHighPrice = 0;
        LastRptSeqLowPrice = 0;
        LastRptSeqLastTradePrice = 0;
        LastRptSeqExecutionStatistics = 0;

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
