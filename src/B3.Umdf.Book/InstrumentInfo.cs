namespace B3.Umdf.Book;

/// <summary>
/// Per-instrument market data beyond the order book (prices, stats, status).
/// All prices are raw SBE mantissa values (multiply by 10^-exponent to get decimal).
/// Enum-like fields use int? to avoid coupling with generated SBE enums.
/// </summary>
public sealed class InstrumentInfo
{
    private long _version;
    private long _securityDefinitionVersion;
    private long _priceBandVersion;
    private long _auctionVersion;

    /// <summary>Monotonic version counter. Bumped by the feed thread on every mutation.</summary>
    public long Version => Volatile.Read(ref _version);

    /// <summary>Increment the version counter after updating fields. Feed-thread-only.</summary>
    public void BumpVersion() => ++_version;

    /// <summary>
    /// Monotonic version counter dedicated to <c>SecurityDefinition_12</c> deltas.
    /// Bumped ONLY by <c>MarketDataManager.HandleSecurityDefinition</c> when the
    /// payload is actually applied (i.e., not in the idempotent re-broadcast
    /// fast-path). Used by the WebSocket fan-out to push
    /// <see cref="B3.Umdf.Server.MessageType.SecurityDefinition"/> frames on real
    /// deltas without re-emitting them on unrelated <see cref="BumpVersion"/>
    /// calls (status/info updates).
    /// </summary>
    public long SecurityDefinitionVersion => Volatile.Read(ref _securityDefinitionVersion);

    /// <summary>Increment the SecurityDefinition version counter. Feed-thread-only.</summary>
    public void BumpSecurityDefinitionVersion() => ++_securityDefinitionVersion;

    /// <summary>
    /// Monotonic version counter dedicated to <c>PriceBand_22</c> deltas.
    /// Bumped ONLY by <c>MarketDataManager.HandlePriceBand</c> when the band
    /// values actually change (the venue may re-broadcast the same band
    /// periodically). Used by the WebSocket fan-out to push
    /// <see cref="B3.Umdf.Server.MessageType.PriceBand"/> frames on real
    /// deltas without re-emitting them on unrelated <see cref="BumpVersion"/>
    /// calls (status / VWAP / candle updates).
    /// </summary>
    public long PriceBandVersion => Volatile.Read(ref _priceBandVersion);

    /// <summary>Increment the PriceBand version counter. Feed-thread-only.</summary>
    public void BumpPriceBandVersion() => ++_priceBandVersion;

    /// <summary>
    /// Monotonic version counter dedicated to auction state deltas
    /// (<c>AuctionImbalance_19</c> + <c>SecurityGroupPhase_10</c>).
    /// Bumped ONLY when the imbalance or group-phase fields actually change.
    /// Used by the WebSocket fan-out to push
    /// <see cref="B3.Umdf.Server.MessageType.Auction"/> frames on real
    /// deltas without re-emitting them on unrelated <see cref="BumpVersion"/>
    /// calls (status / price updates).
    /// </summary>
    public long AuctionVersion => Volatile.Read(ref _auctionVersion);

    /// <summary>Increment the Auction version counter. Feed-thread-only.</summary>
    public void BumpAuctionVersion() => ++_auctionVersion;

    /// <summary>
    /// Last observed <c>SecurityValidityTimestamp</c> from
    /// <c>SecurityDefinition_12</c>, in seconds since epoch. The exchange bumps
    /// this field only when the definition actually changes (corporate action,
    /// contract adjustment, new listing). The InstrDef channel re-broadcasts
    /// every SecDef every few seconds, so caching the last-seen value lets
    /// <c>MarketDataManager.HandleSecurityDefinition</c> short-circuit the
    /// entire parse + 6 string allocations + 4 handler delegates + closure when
    /// nothing changed (~99 % of invocations under steady state). Default 0
    /// means "never observed" — always parse on first sight.
    /// Feed-thread-only writer (one MDM per group, dispatched single-thread by
    /// <see cref="B3.Umdf.Feed.MultiFeedManager"/>); reads from other threads
    /// are tolerated as a best-effort optimization.
    /// </summary>
    public ulong LastSecurityValidityTimestamp { get; set; }

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
    /// <summary>
    /// Minimum trade volume (lot size) from <c>SecurityDefinition_12.MinTradeVol</c>
    /// — the smallest order size the venue accepts for this instrument. Raw
    /// integer (no Fixed8 exponent). Required by downstream pre-trade tick/lot
    /// guards (see issue #55 / B3TradingPlatform#454).
    /// </summary>
    public long? MinTradeVolume { get; set; }
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
    /// <summary>
    /// Raw <c>ImbalanceCondition</c> bitfield from <c>AuctionImbalance_19</c>
    /// (SBE schema 2.2.0 §AuctionImbalance). Two bits are defined:
    /// <c>0x0100</c> = ImbalanceMoreBuyers (P), <c>0x0200</c> = ImbalanceMoreSellers (Q).
    /// All bits off = BALANCED. Kept as raw <c>ushort</c> so new bits added
    /// upstream stay forward-compatible.
    /// </summary>
    public ushort? AuctionImbalanceCondition { get; set; }

    /// <summary><c>AuctionImbalance_19.MDEntryTimestamp</c> (UTC nanoseconds since
    /// epoch). Timestamp of the last auction imbalance update.</summary>
    public long? AuctionTimestamp { get; set; }

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

    /// <summary>SBE PriceBandType (tag 1305) from <c>PriceBand_22</c>. Discriminator
    /// for hard vs. soft / continuous vs. auction bands. Null when the venue did
    /// not specify the band classification.</summary>
    public byte? PriceBandType { get; set; }

    /// <summary>SBE PriceBandMidpointPriceType from <c>PriceBand_22</c>. Only
    /// emitted for Rejection / Auction bands when <see cref="PriceLimitType"/>
    /// equals PERCENTAGE; null otherwise.</summary>
    public byte? PriceBandMidpointPriceType { get; set; }

    /// <summary><c>PriceBand_22.MDEntryTimestamp</c> (UTC nanoseconds since
    /// epoch). Lets consumers age out stale bands independently of
    /// <see cref="LastUpdateTimestamp"/> which is shared across stat templates.</summary>
    public long? PriceBandTimestamp { get; set; }
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
        MinTradeVolume = null;
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
        AuctionImbalanceCondition = null;
        AuctionTimestamp = null;
        PriceBandLow = null;
        PriceBandHigh = null;
        PriceLimitType = null;
        TradingReferencePrice = null;
        PriceBandType = null;
        PriceBandMidpointPriceType = null;
        PriceBandTimestamp = null;
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
        AuctionImbalanceCondition = null;

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
