using B3.Umdf.Server.Configuration;

namespace B3.Umdf.Server;

/// <summary>
/// Cohesive option projections over <see cref="AppSettings"/>. The class keeps
/// its flat property bag (preserved for the JSON-source-gen contract,
/// environment-variable overrides, and existing tests/consumers) and exposes
/// these helpers so downstream components can take dependencies on a small,
/// purpose-built record instead of the whole bag.
/// </summary>
public sealed partial class AppSettings
{
    /// <summary>
    /// Snapshot the connection / per-client backpressure knobs into an
    /// immutable record suitable for passing to the WebSocket host or the
    /// outlier sweeper.
    /// </summary>
    public ConnectionLimitsOptions GetConnectionLimits() => new(
        MaxConnections: MaxConnections,
        ClientChannelCapacity: ClientChannelCapacity,
        SlowClientThreshold: SlowClientThreshold,
        SlowClientMaxTicks: SlowClientMaxTicks,
        ClientMaxPendingBytes: ClientMaxPendingBytes,
        ClientOutlierMultiplier: ClientOutlierMultiplier,
        ClientOutlierMinBytes: ClientOutlierMinBytes,
        ClientOutlierPressurePct: ClientOutlierPressurePct,
        ClientOutlierIntervalMs: ClientOutlierIntervalMs,
        ClientCoalesceWindowMs: ClientCoalesceWindowMs);

    /// <summary>
    /// Snapshot the /health staleness gate plus shutdown drain budget.
    /// </summary>
    public RecoveryHealthOptions GetRecoveryHealthOptions() => new(
        HealthMaxStaleSeconds: HealthMaxStaleSeconds,
        HealthFailOnRecovery: HealthFailOnRecovery,
        ShutdownDrainSeconds: ShutdownDrainSeconds);

    /// <summary>
    /// Snapshot the buffering / dispatch knobs (feed/group rings, conflation
    /// window, stale-MBO ladder, per-symbol fanout suppression watermarks).
    /// </summary>
    public BufferingOptions GetBufferingOptions() => new(
        FeedChannelCapacity: FeedChannelCapacity,
        GroupRingCapacity: GroupRingCapacity,
        MulticastMergeCapacity: MulticastMergeCapacity,
        ServerFlushWindowMs: ServerFlushWindowMs,
        MaxSnapshotRequestsPerBatch: MaxSnapshotRequestsPerBatch,
        StaleBufferGlobalMib: StaleBufferGlobalMib,
        StaleBufferCapLevels: StaleBufferCapLevels,
        StaleEscapeTimeoutMs: StaleEscapeTimeoutMs,
        PerSymbolFanoutSuppressHighPct: PerSymbolFanoutSuppressHighPct,
        PerSymbolFanoutSuppressLowPct: PerSymbolFanoutSuppressLowPct);

    /// <summary>
    /// Snapshot the replay-driver knobs (speed, multicast republish target,
    /// PCAP inputs, loss-injection profile).
    /// </summary>
    public ReplayOptions GetReplayOptions() => new(
        Speed: Speed,
        ReplayToMulticast: ReplayToMulticast,
        PcapPrefixes: PcapPrefixes.AsReadOnly(),
        PcapDirectory: PcapDirectory,
        MulticastConfig: MulticastConfig,
        LossTargets: LossTargets,
        LossRate: LossRate,
        LossMode: LossMode,
        LossBurstSize: LossBurstSize,
        LossCorrelated: LossCorrelated,
        LossSeed: LossSeed);
}
