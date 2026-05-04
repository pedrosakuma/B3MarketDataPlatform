namespace B3.Umdf.Server.Configuration;

/// <summary>
/// Connection / per-client backpressure tuning projected from
/// <see cref="AppSettings"/>. These values shape the WebSocket host's
/// admission control (<c>MaxConnections</c>) and the per-client outbound
/// pipeline (channel capacity, slow-client detection, byte cap, coalescing
/// window). Built via <see cref="AppSettings.GetConnectionLimits"/>.
/// </summary>
public sealed record ConnectionLimitsOptions(
    int MaxConnections,
    int ClientChannelCapacity,
    double SlowClientThreshold,
    int SlowClientMaxTicks,
    long ClientMaxPendingBytes,
    double ClientOutlierMultiplier,
    long ClientOutlierMinBytes,
    double ClientOutlierPressurePct,
    int ClientOutlierIntervalMs,
    int ClientCoalesceWindowMs);

/// <summary>
/// /health staleness gate + graceful-drain budget. Built via
/// <see cref="AppSettings.GetRecoveryHealthOptions"/>.
/// </summary>
public sealed record RecoveryHealthOptions(
    int HealthMaxStaleSeconds,
    bool HealthFailOnRecovery,
    int ShutdownDrainSeconds);

/// <summary>
/// Buffering knobs governing the dispatch path: per-group MPSC ring,
/// downstream channel capacity, server-side conflation flush window,
/// per-batch snapshot dispatch cap, and stale-MBO buffer caps. Built via
/// <see cref="AppSettings.GetBufferingOptions"/>.
/// </summary>
public sealed record BufferingOptions(
    int FeedChannelCapacity,
    int GroupRingCapacity,
    int MulticastMergeCapacity,
    int ServerFlushWindowMs,
    int MaxSnapshotRequestsPerBatch,
    int StaleBufferGlobalMib,
    int[] StaleBufferCapLevels,
    long StaleEscapeTimeoutMs,
    double PerSymbolFanoutSuppressHighPct,
    double PerSymbolFanoutSuppressLowPct);

/// <summary>
/// Replay / loss-injection switches consumed by the ConsoleApp's PCAP
/// driver and <c>LossPolicyFactory</c>. Built via
/// <see cref="AppSettings.GetReplayOptions"/>.
/// </summary>
public sealed record ReplayOptions(
    double Speed,
    bool ReplayToMulticast,
    IReadOnlyList<string> PcapPrefixes,
    string PcapDirectory,
    string? MulticastConfig,
    string? LossTargets,
    double LossRate,
    string LossMode,
    int LossBurstSize,
    bool LossCorrelated,
    int LossSeed);
