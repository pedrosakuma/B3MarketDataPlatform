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

[JsonSerializable(typeof(HealthResponse))]
[JsonSerializable(typeof(SymbolsResponse))]
[JsonSerializable(typeof(TopResponse))]
[JsonSerializable(typeof(BookDiagResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public partial class AppJsonContext : JsonSerializerContext;
