using System.Text.Json;
using System.Text.Json.Serialization;

namespace B3.Umdf.Server;

/// <summary>
/// Application settings that can be loaded from appsettings.json, 
/// environment variables, or overridden via CLI arguments.
/// </summary>
public sealed class AppSettings
{
    /// <summary>WebSocket server port. CLI: --ws-port</summary>
    public int? WsPort { get; set; }

    /// <summary>Replay speed multiplier. 0=max, 1=real-time. CLI: --speed</summary>
    public double Speed { get; set; }

    /// <summary>
    /// Publish replayed PCAP packets to multicast instead of consuming them in-process.
    /// Requires PCAP inputs plus MulticastConfig. CLI: --replay-to-multicast
    /// </summary>
    public bool ReplayToMulticast { get; set; }

    /// <summary>Maximum WebSocket connections allowed (0 = unlimited).</summary>
    public int MaxConnections { get; set; }

    /// <summary>Per-client outbound channel capacity.</summary>
    public int ClientChannelCapacity { get; set; } = 4096;

    /// <summary>Slow client detection: queue depth threshold (0.0-1.0).</summary>
    public double SlowClientThreshold { get; set; } = 0.75;

    /// <summary>Slow client detection: consecutive ticks before disconnect.</summary>
    public int SlowClientMaxTicks { get; set; } = 100;

    /// <summary>Graceful shutdown drain timeout in seconds.</summary>
    public int ShutdownDrainSeconds { get; set; } = 5;

    /// <summary>Capacity of the live multicast merge queue shared across sockets.</summary>
    public int MulticastMergeCapacity { get; set; } = 1_000_000;

    /// <summary>Capacity of each per-group feed queue behind the dispatcher.</summary>
    public int FeedChannelCapacity { get; set; } = 250_000;

    /// <summary>
    /// Cap of the per-group recovery bridge buffer (incrementals deferred while a snapshot
    /// completes). Default 50,000 (~75 MB pinned per group) bounds memory at the cost of
    /// dropping oldest incrementals during long recovery cycles. Raise for high-rate
    /// replay scenarios (e.g. UMDF_SPEED=0) where snapshot wall-time is dominated by
    /// receive-side serialization. CLI: env UMDF_INCREMENTAL_RECOVERY_QUEUE_CAPACITY.
    /// </summary>
    public int IncrementalRecoveryQueueCapacity { get; set; } = 50_000;

    /// <summary>
    /// Per-group MPSC dispatch ring capacity (slots). Default 65 536. Producers (recv
    /// threads) drop newest packets when full; downstream gap detection triggers
    /// snapshot recovery. Raise for high-rate replay (e.g. UMDF_SPEED=0) where the
    /// dispatch thread can briefly fall behind the receive side. CLI: env
    /// UMDF_GROUP_RING_CAPACITY.
    /// </summary>
    public int GroupRingCapacity { get; set; } = 65_536;

    /// <summary>Minimum log level (Trace, Debug, Information, Warning, Error, Critical).</summary>
    public string LogLevel { get; set; } = "Information";

    /// <summary>PCAP prefix paths (repeatable). CLI: --pcap-prefix</summary>
    public List<string> PcapPrefixes { get; set; } = new();

    /// <summary>Multicast config JSON path. CLI: --multicast-config</summary>
    public string? MulticastConfig { get; set; }

    /// <summary>Load settings from a JSON file.</summary>
    public static AppSettings LoadFromFile(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize(json, AppSettingsJsonContext.Default.AppSettings) ?? new AppSettings();
    }

    /// <summary>Load from default appsettings.json if present, otherwise default.</summary>
    public static AppSettings LoadDefault()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (File.Exists(path))
            return LoadFromFile(path);

        // Also check working directory
        if (File.Exists("appsettings.json"))
            return LoadFromFile("appsettings.json");

        return new AppSettings();
    }

    /// <summary>Apply environment variable overrides.</summary>
    public void ApplyEnvironment()
    {
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_WS_PORT"), out var port))
            WsPort = port;
        if (double.TryParse(Environment.GetEnvironmentVariable("UMDF_SPEED"), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var sp))
            Speed = sp;
        if (TryParseBoolean(Environment.GetEnvironmentVariable("UMDF_REPLAY_TO_MULTICAST"), out var replayToMulticast))
            ReplayToMulticast = replayToMulticast;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_MAX_CONNECTIONS"), out var mc))
            MaxConnections = mc;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_CHANNEL_CAPACITY"), out var cc))
            ClientChannelCapacity = cc;
        if (double.TryParse(Environment.GetEnvironmentVariable("UMDF_SLOW_CLIENT_THRESHOLD"), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var slowThreshold))
            SlowClientThreshold = slowThreshold;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_SLOW_CLIENT_MAX_TICKS"), out var slowTicks))
            SlowClientMaxTicks = slowTicks;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_SHUTDOWN_DRAIN_SECONDS"), out var sd))
            ShutdownDrainSeconds = sd;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_MULTICAST_MERGE_CAPACITY"), out var mergeCapacity))
            MulticastMergeCapacity = mergeCapacity;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_FEED_CHANNEL_CAPACITY"), out var feedCapacity))
            FeedChannelCapacity = feedCapacity;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_INCREMENTAL_RECOVERY_QUEUE_CAPACITY"), out var recCapacity))
            IncrementalRecoveryQueueCapacity = recCapacity;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_GROUP_RING_CAPACITY"), out var ringCapacity))
            GroupRingCapacity = ringCapacity;
        var logLevel = Environment.GetEnvironmentVariable("UMDF_LOG_LEVEL");
        if (!string.IsNullOrEmpty(logLevel))
            LogLevel = logLevel;
        var multicast = Environment.GetEnvironmentVariable("UMDF_MULTICAST_CONFIG");
        if (!string.IsNullOrEmpty(multicast))
            MulticastConfig = multicast;
    }

    private static bool TryParseBoolean(string? value, out bool result)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            result = false;
            return false;
        }

        switch (value.Trim().ToLowerInvariant())
        {
            case "1":
            case "true":
            case "yes":
            case "on":
                result = true;
                return true;

            case "0":
            case "false":
            case "no":
            case "off":
                result = false;
                return true;

            default:
                result = false;
                return false;
        }
    }
}

[JsonSerializable(typeof(AppSettings))]
[JsonSourceGenerationOptions(
    PropertyNameCaseInsensitive = true,
    ReadCommentHandling = JsonCommentHandling.Skip,
    AllowTrailingCommas = true)]
internal partial class AppSettingsJsonContext : JsonSerializerContext;
