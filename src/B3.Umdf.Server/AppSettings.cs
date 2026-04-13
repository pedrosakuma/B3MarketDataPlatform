using System.Text.Json;

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

    /// <summary>Minimum log level (Trace, Debug, Information, Warning, Error, Critical).</summary>
    public string LogLevel { get; set; } = "Information";

    /// <summary>PCAP prefix paths (repeatable). CLI: --pcap-prefix</summary>
    public List<string> PcapPrefixes { get; set; } = new();

    /// <summary>Multicast config JSON path. CLI: --multicast-config</summary>
    public string? MulticastConfig { get; set; }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    /// <summary>Load settings from a JSON file.</summary>
    public static AppSettings LoadFromFile(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<AppSettings>(json, JsonOptions) ?? new AppSettings();
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
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_MAX_CONNECTIONS"), out var mc))
            MaxConnections = mc;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_CHANNEL_CAPACITY"), out var cc))
            ClientChannelCapacity = cc;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_SHUTDOWN_DRAIN_SECONDS"), out var sd))
            ShutdownDrainSeconds = sd;
        var logLevel = Environment.GetEnvironmentVariable("UMDF_LOG_LEVEL");
        if (!string.IsNullOrEmpty(logLevel))
            LogLevel = logLevel;
        var multicast = Environment.GetEnvironmentVariable("UMDF_MULTICAST_CONFIG");
        if (!string.IsNullOrEmpty(multicast))
            MulticastConfig = multicast;
    }
}
