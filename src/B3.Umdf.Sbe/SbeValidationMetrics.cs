using System.Diagnostics.Metrics;

namespace B3.Umdf.Sbe;

/// <summary>
/// Defensive-validation counters for issue #15. Published on the shared <c>"B3.Umdf"</c> meter
/// so the existing OTLP / Prometheus pipeline (see <c>MetricsRegistry</c> in B3.Umdf.Server)
/// picks them up automatically without a project reference cycle.
/// </summary>
public static class SbeValidationMetrics
{
    public static readonly Meter Meter = new("B3.Umdf", "1.0.0");

    /// <summary>Incremented when the SBE message header fails schema/version validation.</summary>
    public static readonly Counter<long> HeaderMismatches =
        Meter.CreateCounter<long>("umdf.sbe.header_mismatches");

    /// <summary>Incremented when the message header cannot even be read (buffer &lt; 8 bytes).</summary>
    public static readonly Counter<long> HeaderTruncated =
        Meter.CreateCounter<long>("umdf.sbe.header_truncated");
}
