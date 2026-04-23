namespace B3.Umdf.PcapReplay;

using B3.Umdf.Transport;

public sealed record ReplayOptions
{
    /// <summary>Playback speed multiplier. 0 = burst (no delay).</summary>
    public double SpeedMultiplier { get; init; } = 1.0;

    /// <summary>
    /// Optional packet-loss injector for resilience testing. When null, no loss.
    /// Loss is applied AFTER timestamp pacing — the dropped packet's reader still
    /// advances so subsequent packets keep flowing. The next packet from the same
    /// reader is still enqueued, so loss is granular per-packet, not per-reader.
    /// </summary>
    public LossPolicy? Loss { get; init; }
}

/// <summary>Which channel classes are eligible for drop.</summary>
[System.Flags]
public enum LossTargets
{
    None             = 0,
    IncrementalA     = 1 << 0,
    IncrementalB     = 1 << 1,
    SnapshotRecovery = 1 << 2,
    InstrumentDef    = 1 << 3,
    Incrementals     = IncrementalA | IncrementalB,
    All              = IncrementalA | IncrementalB | SnapshotRecovery | InstrumentDef,
}

public enum LossMode
{
    /// <summary>Independent Bernoulli trial per eligible packet at <c>Rate</c>.</summary>
    Random,
    /// <summary>Drop runs of <c>BurstSize</c> consecutive eligible packets, gated by <c>Rate</c> as the per-packet trigger probability for entering a burst.</summary>
    Burst,
}

/// <summary>
/// Configuration for the replayer's loss injector.
/// </summary>
/// <param name="Targets">Bitmask of channel classes the policy applies to.</param>
/// <param name="Mode">Random Bernoulli vs burst-drop runs.</param>
/// <param name="Rate">Drop probability (Random) or burst-trigger probability (Burst). 0..1.</param>
/// <param name="BurstSize">Number of consecutive packets dropped once a burst triggers (Burst mode only).</param>
/// <param name="Correlated">When true, IncrementalA + IncrementalB drop the SAME logical packet (drives both A and B feeds to lose simultaneously — worst case for A/B arbitration). When false, A and B are decided independently (typical: lost on one, surviving on the other).</param>
/// <param name="Seed">RNG seed for reproducible test runs. Null = nondeterministic.</param>
public sealed record LossPolicy(
    LossTargets Targets,
    LossMode Mode = LossMode.Random,
    double Rate = 0.0,
    int BurstSize = 1,
    bool Correlated = false,
    int? Seed = null
);
