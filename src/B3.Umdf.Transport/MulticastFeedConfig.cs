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
}

[JsonSerializable(typeof(MulticastFeedConfig))]
[JsonSourceGenerationOptions(
    PropertyNameCaseInsensitive = true,
    ReadCommentHandling = JsonCommentHandling.Skip,
    AllowTrailingCommas = true,
    UseStringEnumConverter = true)]
internal partial class FeedConfigJsonContext : JsonSerializerContext;
