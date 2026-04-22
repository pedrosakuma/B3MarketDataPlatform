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

    /// <summary>
    /// Per-client outbound channel capacity (number of pre-serialized batches the broadcaster
    /// can stage ahead of the WebSocket send loop). The send loop drains up to MaxDrainPerCycle
    /// per iteration, coalesces all into one WS frame, then awaits SendAsync. While that send
    /// is in flight the broadcaster keeps producing — at 50k+ packets/s, a single 100 ms WS
    /// write can stage thousands of items. Sized to cover that burst window without false-
    /// positive disconnects; the byte budget (ClientMaxPendingBytes) is the real memory cap.
    /// </summary>
    public int ClientChannelCapacity { get; set; } = 32768;

    /// <summary>Slow client detection: queue depth threshold (0.0-1.0).</summary>
    public double SlowClientThreshold { get; set; } = 0.75;

    /// <summary>Slow client detection: consecutive ticks before disconnect.</summary>
    public int SlowClientMaxTicks { get; set; } = 100;

    /// <summary>
    /// Hard cap (in bytes) of payload pending in a client's outbound ring. Producers
    /// disconnect the client immediately when accepting a new payload would exceed this
    /// budget — guards against multi-MB coalesced batches accumulating into OOM before
    /// the queue-depth threshold (counted in messages) trips. 0 disables the check.
    /// Default 32 MiB is sized to absorb the initial snapshot burst — a deep MBO book
    /// for one B3 active stock can be ~6 MiB on the wire (≈170k orders × 37 B), and a
    /// client typically subscribes to several at once. With 32 MiB per client, even
    /// hundreds of clients fit a ~4 GiB container (e.g. 500 × 32 MiB = 16 GiB upper
    /// bound, but the steady-state pending bytes are ~50 KB once the snapshot has
    /// drained — the cap only matters during the burst). Lower it (e.g. 8–16 MiB) if
    /// memory is constrained and clients are expected to subscribe to ≤2 deep books at
    /// a time. CLI: env UMDF_CLIENT_MAX_PENDING_BYTES.
    /// </summary>
    public long ClientMaxPendingBytes { get; set; } = 32L * 1024 * 1024;

    /// <summary>
    /// Outlier multiplier used by the periodic slow-consumer sweep. A client is a
    /// candidate for forced disconnect when its pending payload bytes exceed
    /// max(median × <c>ClientOutlierMultiplier</c>, <see cref="ClientOutlierMinBytes"/>).
    /// Disabled when ≤ 0. Default 4.0 keeps outlier definition stable across feed
    /// rates: a healthy fleet's pending bytes track the coalescing window, and a
    /// genuinely stuck consumer trails by orders of magnitude. CLI: env
    /// UMDF_CLIENT_OUTLIER_MULTIPLIER.
    /// </summary>
    public double ClientOutlierMultiplier { get; set; } = 4.0;

    /// <summary>
    /// Absolute floor (bytes) below which a client is never disconnected by the
    /// outlier sweep, regardless of multiplier. Prevents killing clients with
    /// trivial pending bytes when the median is near zero. Default 256 KiB.
    /// CLI: env UMDF_CLIENT_OUTLIER_MIN_BYTES.
    /// </summary>
    public long ClientOutlierMinBytes { get; set; } = 256L * 1024;

    /// <summary>
    /// Aggregate-pressure gate (0.0–1.0). The outlier sweep only disconnects when
    /// the sum of all clients' pending bytes exceeds this fraction of the total
    /// budget (<c>ClientCount × ClientMaxPendingBytes</c>). Below the gate, a few
    /// momentarily-slow outliers are harmless and left alone; above it, the
    /// fleet is under real memory pressure and outliers are the most likely
    /// root cause. Default 0.50. CLI: env UMDF_CLIENT_OUTLIER_PRESSURE_PCT.
    /// </summary>
    public double ClientOutlierPressurePct { get; set; } = 0.50;

    /// <summary>
    /// Period (milliseconds) of the outlier sweep timer. 0 disables the sweep
    /// entirely (only the hard <see cref="ClientMaxPendingBytes"/> cap remains in
    /// force). Default 1000 ms keeps overhead negligible (one O(n) scan per
    /// second over connected clients). CLI: env UMDF_CLIENT_OUTLIER_INTERVAL_MS.
    /// </summary>
    public int ClientOutlierIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Coalescing window (milliseconds) the per-client write loop waits AFTER being
    /// woken on the first item before draining + sending. Larger values produce bigger
    /// frames + fewer Kestrel pipe-lock acquisitions at the cost of added latency.
    /// 0 disables (immediate drain). Sweet spot for hundreds of WS clients is 10–20ms;
    /// beyond ~50ms the per-client pending memory grows materially. CLI: env
    /// UMDF_CLIENT_COALESCE_WINDOW_MS.
    /// </summary>
    public int ClientCoalesceWindowMs { get; set; } = 10;

    /// <summary>
    /// Maximum number of subscribe/get snapshot requests the dispatch thread will
    /// service per packet batch. Snapshots allocate (CopyOrderData + ArrayPool rent)
    /// and are queued onto per-client outbound rings. Without a per-batch cap, an
    /// initial flood (e.g. hundreds of clients × 200 symbols on connect) can spike
    /// allocation faster than the GC + write loops can drain, leading to OOM on the
    /// dispatch thread. Excess requests stay in the queue and are drained on
    /// subsequent packets. 0 disables the cap (legacy behavior). CLI: env
    /// UMDF_MAX_SNAPSHOT_REQUESTS_PER_BATCH.
    /// </summary>
    public int MaxSnapshotRequestsPerBatch { get; set; } = 32;

    /// <summary>Graceful shutdown drain timeout in seconds.</summary>
    public int ShutdownDrainSeconds { get; set; } = 5;

    /// <summary>Capacity of the live multicast merge queue shared across sockets.</summary>
    public int MulticastMergeCapacity { get; set; } = 1_000_000;

    /// <summary>Capacity of each per-group feed queue behind the dispatcher.</summary>
    public int FeedChannelCapacity { get; set; } = 250_000;

    /// <summary>
    /// Cap of the per-group recovery bridge buffer (incrementals deferred while a snapshot
    /// cycle completes). Must be sized to outlast the wall-time of one snapshot cycle:
    /// if the earliest packet retained when the snapshot completes is newer than
    /// (snapshot.MinSeqNum + 1), the catch-up step has nothing to bridge with and the
    /// FeedHandler waits for another cycle — re-overflowing the queue with the same
    /// ratio and producing a permanent recovery loop. Empirically, a B3 snapshot for
    /// ~18k symbols takes ~12 s of wall-time; at ~4.5k incrementals/s per group that
    /// requires ~55k slots minimum. Default 200,000 gives ~45 s of head-room. Memory
    /// cost: ~MTU × capacity × groups (200k × 1.5 KiB × 2 ≈ 600 MiB worst case;
    /// typically half that since average B3 packet is ~700 B). CLI: env
    /// UMDF_INCREMENTAL_RECOVERY_QUEUE_CAPACITY.
    /// </summary>
    public int IncrementalRecoveryQueueCapacity { get; set; } = 200_000;

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
        if (long.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_MAX_PENDING_BYTES"), out var maxPending))
            ClientMaxPendingBytes = maxPending;
        if (double.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_OUTLIER_MULTIPLIER"), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var outlierMul))
            ClientOutlierMultiplier = outlierMul;
        if (long.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_OUTLIER_MIN_BYTES"), out var outlierMin))
            ClientOutlierMinBytes = outlierMin;
        if (double.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_OUTLIER_PRESSURE_PCT"), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var outlierPress))
            ClientOutlierPressurePct = outlierPress;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_OUTLIER_INTERVAL_MS"), out var outlierMs))
            ClientOutlierIntervalMs = outlierMs;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_CLIENT_COALESCE_WINDOW_MS"), out var coalesceMs))
            ClientCoalesceWindowMs = coalesceMs;
        if (int.TryParse(Environment.GetEnvironmentVariable("UMDF_MAX_SNAPSHOT_REQUESTS_PER_BATCH"), out var maxSnapPerBatch))
            MaxSnapshotRequestsPerBatch = maxSnapPerBatch;
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
