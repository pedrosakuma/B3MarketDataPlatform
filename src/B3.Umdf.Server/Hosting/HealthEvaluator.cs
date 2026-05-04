using B3.Umdf.Feed;

namespace B3.Umdf.Server.Hosting;

/// <summary>
/// Pure (no IO, no time-source dependency once <c>nowTicks</c> is supplied)
/// staleness evaluator backing the <c>/health</c> endpoint. Extracted from
/// <see cref="WebSocketHost"/> so the rule set ("a non-Streaming group whose
/// last packet is older than the threshold flips the response to 503") can
/// be unit-tested without spinning up Kestrel.
/// </summary>
public static class HealthEvaluator
{
    /// <summary>
    /// Returns the "<c>group=state for Ns</c>" descriptors for every group
    /// that is considered unhealthy under the stale-recovery rule, or
    /// <c>null</c> when the response should remain 200 (either because the
    /// gate is disabled, no per-group state was provided, or every group is
    /// either Streaming or fresh enough).
    /// </summary>
    /// <param name="states">Per-group feed state map (e.g. "Streaming",
    /// "WaitInstrumentDefinition"). When <c>null</c>, returns <c>null</c>.</param>
    /// <param name="lastPacketTicks">Per-group last-observed-packet timestamp
    /// (<see cref="Environment.TickCount64"/> units). May be <c>null</c>; for
    /// groups absent from the map the staleness window falls back to
    /// <paramref name="uptimeSeconds"/> (cold-start semantics).</param>
    /// <param name="nowTicks"><see cref="Environment.TickCount64"/> at the
    /// moment of evaluation.</param>
    /// <param name="uptimeSeconds">Process uptime in seconds (used as the
    /// fallback staleness window for groups with no recorded packet).</param>
    /// <param name="maxStaleSeconds">Threshold beyond which a non-Streaming
    /// group is considered unhealthy. Negative values disable the gate.</param>
    /// <param name="failOnRecovery">Master switch matching
    /// <c>AppSettings.HealthFailOnRecovery</c>. When <c>false</c>, the
    /// evaluator unconditionally returns <c>null</c>.</param>
    public static List<string>? FindUnhealthyGroups(
        IReadOnlyDictionary<string, string>? states,
        IReadOnlyDictionary<string, long>? lastPacketTicks,
        long nowTicks,
        double uptimeSeconds,
        int maxStaleSeconds,
        bool failOnRecovery)
    {
        if (!failOnRecovery || states is null || maxStaleSeconds < 0)
            return null;

        List<string>? unhealthy = null;
        foreach (var (group, state) in states)
        {
            if (string.Equals(state, nameof(FeedState.Streaming), StringComparison.OrdinalIgnoreCase))
                continue;

            double staleSec;
            if (lastPacketTicks is not null && lastPacketTicks.TryGetValue(group, out var ticks) && ticks > 0)
                staleSec = (nowTicks - ticks) / 1000.0;
            else
                staleSec = uptimeSeconds;

            if (staleSec > maxStaleSeconds)
            {
                unhealthy ??= new List<string>();
                unhealthy.Add($"{group}={state} for {staleSec:F0}s");
            }
        }
        return unhealthy;
    }
}
