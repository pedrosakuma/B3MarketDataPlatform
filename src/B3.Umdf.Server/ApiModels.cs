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

[JsonSerializable(typeof(HealthResponse))]
[JsonSerializable(typeof(SymbolsResponse))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public partial class AppJsonContext : JsonSerializerContext;
