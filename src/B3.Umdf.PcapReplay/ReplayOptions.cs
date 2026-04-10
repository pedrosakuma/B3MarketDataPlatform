namespace B3.Umdf.PcapReplay;

public sealed record ReplayOptions
{
    /// <summary>Playback speed multiplier. 0 = burst (no delay).</summary>
    public double SpeedMultiplier { get; init; } = 1.0;
}
