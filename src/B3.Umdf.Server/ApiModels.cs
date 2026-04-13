using System.Text.Json.Serialization;

namespace B3.Umdf.Server;

public sealed class HealthResponse
{
    public string Status { get; set; } = "";
    public string Uptime { get; set; } = "";
    public long SlowClientDisconnects { get; set; }
    public Dictionary<string, string>? FeedGroups { get; set; }
    public Dictionary<string, long>? LastPacketTimestamps { get; set; }
}

public sealed class SymbolsResponse
{
    public int Count { get; set; }
    public int Matched { get; set; }
    public string[] Symbols { get; set; } = [];
}

public sealed class TopInstrument
{
    public string Symbol { get; set; } = "";
    public ulong SecurityId { get; set; }
    public int BidOrders { get; set; }
    public int AskOrders { get; set; }
    public int BidLevels { get; set; }
    public int AskLevels { get; set; }
}

public sealed class TopResponse
{
    public int TotalBooks { get; set; }
    public TopInstrument[] Instruments { get; set; } = [];
}

public sealed class BookDiagResponse
{
    public string Symbol { get; set; } = "";
    public ulong SecurityId { get; set; }
    public long BestBid { get; set; }
    public long BestAsk { get; set; }
    public int BidOrders { get; set; }
    public int AskOrders { get; set; }
    public int BidLevels { get; set; }
    public int AskLevels { get; set; }
    public uint LastRptSeq { get; set; }
    public bool Crossed { get; set; }
    public string[] ValidationErrors { get; set; } = [];
}

public sealed class FeedStatsResponse
{
    public long OrderAdds { get; set; }
    public long OrderUpdates { get; set; }
    public long OrderDeletes { get; set; }
    public long DeleteNotFound { get; set; }
    public long NullPriceNewSkips { get; set; }
    public long NullPriceChangeDeletes { get; set; }
    public long ParseErrors { get; set; }
    public long CrossingTransitions { get; set; }
    public Dictionary<string, ChannelStats>? ChannelStats { get; set; }
    public int CrossedBooks { get; set; }
    public string[] CrossedSymbols { get; set; } = [];
}

public sealed class ChannelStats
{
    public long PacketsProcessed { get; set; }
    public long DuplicatesSkipped { get; set; }
    public long GapsDetected { get; set; }
    public uint ExpectedSeqNum { get; set; }
}

[JsonSerializable(typeof(HealthResponse))]
[JsonSerializable(typeof(SymbolsResponse))]
[JsonSerializable(typeof(TopResponse))]
[JsonSerializable(typeof(BookDiagResponse))]
[JsonSerializable(typeof(FeedStatsResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public partial class AppJsonContext : JsonSerializerContext;
