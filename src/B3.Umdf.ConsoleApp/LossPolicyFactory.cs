using B3.Umdf.PcapReplay;
using B3.Umdf.Server;

namespace B3.Umdf.ConsoleApp;

/// <summary>
/// Translates loss-injection settings from <see cref="AppSettings"/> (string-typed,
/// to keep JSON / env wiring simple) into the strongly-typed <see cref="LossPolicy"/>
/// consumed by the replayer. Returns null when loss is disabled.
/// </summary>
internal static class LossPolicyFactory
{
    public static LossPolicy? FromSettings(AppSettings s)
    {
        if (string.IsNullOrWhiteSpace(s.LossTargets)) return null;

        var targets = ParseTargets(s.LossTargets);
        if (targets == LossTargets.None) return null;
        if (s.LossRate <= 0) return null;

        var mode = string.Equals(s.LossMode, "burst", StringComparison.OrdinalIgnoreCase)
            ? LossMode.Burst
            : LossMode.Random;

        return new LossPolicy(
            Targets:    targets,
            Mode:       mode,
            Rate:       s.LossRate,
            BurstSize:  Math.Max(1, s.LossBurstSize),
            Correlated: s.LossCorrelated,
            Seed:       s.LossSeed != 0 ? s.LossSeed : null
        );
    }

    private static LossTargets ParseTargets(string spec)
    {
        var result = LossTargets.None;
        foreach (var raw in spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            result |= raw.ToLowerInvariant() switch
            {
                "a" or "inca" or "incrementala"     => LossTargets.IncrementalA,
                "b" or "incb" or "incrementalb"     => LossTargets.IncrementalB,
                "ab" or "incab" or "incrementals"   => LossTargets.Incrementals,
                "snap" or "snapshot" or "recovery"  => LossTargets.SnapshotRecovery,
                "instr" or "instrdef" or "definition" => LossTargets.InstrumentDef,
                "all"                                => LossTargets.All,
                "none" or ""                         => LossTargets.None,
                _ => LossTargets.None,
            };
        }
        return result;
    }

    public static string Describe(LossPolicy p) =>
        $"targets={p.Targets} rate={p.Rate:P1} mode={p.Mode}" +
        (p.Mode == LossMode.Burst ? $" burst={p.BurstSize}" : "") +
        (p.Correlated ? " correlated" : "") +
        (p.Seed is { } s ? $" seed={s}" : "");
}
