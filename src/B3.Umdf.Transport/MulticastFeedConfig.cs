using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace B3.Umdf.Transport;

/// <summary>
/// JSON-loadable configuration for multicast UMDF channels.
/// Each group represents a product (e.g. EQT, DRV) with 4 channel types.
/// </summary>
public sealed class MulticastFeedConfig
{
    [JsonPropertyName("channelGroups")]
    public List<ChannelGroupConfig> ChannelGroups { get; set; } = [];

    /// <summary>
    /// Loads config from a JSON file.
    /// </summary>
    public static MulticastFeedConfig Load(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize(json, FeedConfigJsonContext.Default.MulticastFeedConfig)
            ?? throw new InvalidOperationException($"Failed to parse multicast config: {path}");
    }

    /// <summary>
    /// Builds flat list of <see cref="ChannelConfig"/> from all groups.
    /// </summary>
    public List<ChannelConfig> ToChannelConfigs()
    {
        var configs = new List<ChannelConfig>();
        for (int g = 0; g < ChannelGroups.Count; g++)
        {
            var group = ChannelGroups[g];
            foreach (var ch in group.Channels)
            {
                configs.Add(new ChannelConfig(
                    ChannelId: ch.ChannelId,
                    Type: ch.Type,
                    MulticastGroup: IPAddress.Parse(ch.MulticastGroup),
                    Port: ch.Port,
                    SourceAddress: ch.SourceAddress is not null ? IPAddress.Parse(ch.SourceAddress) : null,
                    LocalAddress: ch.LocalAddress is not null ? IPAddress.Parse(ch.LocalAddress) : null,
                    ReceiveBufferBytes: ch.ReceiveBufferBytes ?? DefaultReceiveBufferBytesFor(ch.Type),
                    ChannelGroup: g,
                    ReceiveSocketCount: Math.Max(1, ch.ReceiveSocketCount ?? 1)));
            }
        }
        return configs;
    }

    /// <summary>
    /// Builds flat list of publish routes from all groups for PCAP replay to multicast.
    /// Reuses the same JSON topology as live consume mode, but ignores receive-only fields
    /// such as sourceAddress and receiveBufferBytes.
    /// </summary>
    public List<MulticastPublishChannelConfig> ToPublishChannelConfigs()
    {
        var configs = new List<MulticastPublishChannelConfig>();
        for (int g = 0; g < ChannelGroups.Count; g++)
        {
            var group = ChannelGroups[g];
            foreach (var ch in group.Channels)
            {
                configs.Add(new MulticastPublishChannelConfig(
                    ChannelId: ch.ChannelId,
                    Type: ch.Type,
                    MulticastGroup: IPAddress.Parse(ch.MulticastGroup),
                    Port: ch.Port,
                    LocalAddress: ch.LocalAddress is not null ? IPAddress.Parse(ch.LocalAddress) : null,
                    ChannelGroup: g));
            }
        }
        return configs;
    }

    /// <summary>
    /// Returns the group IDs (0-based).
    /// </summary>
    public List<int> GetGroupIds() =>
        Enumerable.Range(0, ChannelGroups.Count).ToList();

    /// <summary>
    /// Per-channel-type default receive buffer sizing. Sized to the expected burst profile
    /// for B3 UMDF feeds: incrementals are the hot path and need the largest buffer; snapshot
    /// recovery is bursty but only active during recovery; instrument definition is low-rate
    /// and idempotent. All values can be overridden per channel via <c>receiveBufferBytes</c>.
    /// Note: actual size is capped by the kernel at <c>net.core.rmem_max</c>.
    /// </summary>
    public static int DefaultReceiveBufferBytesFor(ChannelType type) => type switch
    {
        ChannelType.IncrementalA          => 16 * 1024 * 1024,  // 16 MiB — high-rate, bursty, gap-critical
        ChannelType.IncrementalB          => 16 * 1024 * 1024,  // 16 MiB — redundant feed of A, same profile
        ChannelType.SnapshotRecovery      =>  8 * 1024 * 1024,  //  8 MiB — idle in RealTime, heavy during recovery
        ChannelType.InstrumentDefinition  =>  2 * 1024 * 1024,  //  2 MiB — low-rate, periodic, idempotent
        _                                 =>  4 * 1024 * 1024,
    };
}

public sealed class ChannelGroupConfig
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("channels")]
    public List<ChannelEntryConfig> Channels { get; set; } = [];
}

public sealed class ChannelEntryConfig
{
    [JsonPropertyName("channelId")]
    public int ChannelId { get; set; }

    [JsonPropertyName("type")]
    public ChannelType Type { get; set; }

    [JsonPropertyName("multicastGroup")]
    public string MulticastGroup { get; set; } = "";

    [JsonPropertyName("port")]
    public int Port { get; set; }

    [JsonPropertyName("sourceAddress")]
    public string? SourceAddress { get; set; }

    [JsonPropertyName("localAddress")]
    public string? LocalAddress { get; set; }

    [JsonPropertyName("receiveBufferBytes")]
    public int? ReceiveBufferBytes { get; set; }

    /// <summary>
    /// Number of UDP sockets to bind for this channel using SO_REUSEPORT (Linux).
    /// The kernel load-balances datagrams across all sockets, multiplying the
    /// effective per-socket receive buffer and parallelizing receive work.
    /// Default 1 (single socket). Recommended 2-4 for live ingest under burst.
    /// </summary>
    [JsonPropertyName("receiveSocketCount")]
    public int? ReceiveSocketCount { get; set; }
}

[JsonSerializable(typeof(MulticastFeedConfig))]
[JsonSourceGenerationOptions(
    PropertyNameCaseInsensitive = true,
    ReadCommentHandling = JsonCommentHandling.Skip,
    AllowTrailingCommas = true,
    UseStringEnumConverter = true)]
internal partial class FeedConfigJsonContext : JsonSerializerContext;
