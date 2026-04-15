using System.Text.Json.Serialization;

namespace B3.Umdf.Server;

public sealed class HealthResponse
{
    public string Status { get; set; } = "";
    public string Uptime { get; set; } = "";
    public Dictionary<string, string>? FeedGroups { get; set; }
    public Dictionary<string, long>? LastPacketTimestamps { get; set; }
}

public sealed class SymbolsResponse
{
    public int Count { get; set; }
    public int Matched { get; set; }
    public string[] Symbols { get; set; } = [];
}

public sealed class InstrumentInfoResponse
{
    public ulong SecurityId { get; set; }
    public string? Symbol { get; set; }
    public string? Asset { get; set; }
    public string? IsinNumber { get; set; }
    public string? Currency { get; set; }
    public string? CfiCode { get; set; }
    public string? SecurityGroup { get; set; }
    public string? SecurityDescription { get; set; }
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
    public int? TradingStatus { get; set; }
    public int? TradingEvent { get; set; }
    public long? OpeningPrice { get; set; }
    public long? ClosingPrice { get; set; }
    public long? HighPrice { get; set; }
    public long? LowPrice { get; set; }
    public long? LastTradePrice { get; set; }
    public long? LastTradeSize { get; set; }
    public long? SettlementPrice { get; set; }
    public long? TheoreticalOpeningPrice { get; set; }
    public long? TheoreticalOpeningSize { get; set; }
    public long? AuctionImbalanceSize { get; set; }
    public long? PriceBandLow { get; set; }
    public long? PriceBandHigh { get; set; }
    public long? TradingReferencePrice { get; set; }
    public long? AvgDailyTradedQty { get; set; }
    public long? MaxTradeVol { get; set; }
    public long? TradeVolume { get; set; }
    public long? VwapPrice { get; set; }
    public long? NetChangeFromPrevDay { get; set; }
    public long? NumberOfTrades { get; set; }
    public long? OpenInterest { get; set; }
    public ulong LastUpdateTimestamp { get; set; }
    public List<UnderlyingResponse>? Underlyings { get; set; }
    public List<LegResponse>? Legs { get; set; }
    public List<InstrAttribResponse>? InstrAttribs { get; set; }
}

public sealed class UnderlyingResponse
{
    public ulong SecurityId { get; set; }
    public string? Symbol { get; set; }
}

public sealed class LegResponse
{
    public ulong SecurityId { get; set; }
    public string? Symbol { get; set; }
    public long? RatioQty { get; set; }
    public int? SecurityType { get; set; }
    public int? Side { get; set; }
}

public sealed class InstrAttribResponse
{
    public int Type { get; set; }
    public int Value { get; set; }
}

[JsonSerializable(typeof(HealthResponse))]
[JsonSerializable(typeof(SymbolsResponse))]
[JsonSerializable(typeof(InstrumentInfoResponse))]
[JsonSerializable(typeof(UnderlyingResponse))]
[JsonSerializable(typeof(LegResponse))]
[JsonSerializable(typeof(InstrAttribResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public partial class AppJsonContext : JsonSerializerContext;
