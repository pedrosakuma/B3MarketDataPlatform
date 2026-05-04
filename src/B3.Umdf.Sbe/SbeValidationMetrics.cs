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

    /// <summary>
    /// Incremented when a buffer carries a <c>TemplateId</c> the bundled SBE generator does not
    /// know how to decode (issue #15 / fuzz P2). Untagged because the universe of unknown
    /// template ids is the entire <see cref="ushort"/> space (~65 500 values) and tagging
    /// would explode the metrics cardinality. Use <see cref="ValidatingSbeDispatcher.OnUnsupportedTemplate"/>
    /// to receive a sampled callback per unique offending id (rate-limited inside the dispatcher).
    /// </summary>
    public static readonly Counter<long> UnsupportedTemplateCount =
        Meter.CreateCounter<long>("umdf.sbe.unsupported_template_count");

    /// <summary>
    /// Incremented when a known message template fails its generated <c>TryParse</c> (varData length
    /// claim exceeds the buffer, truncated composite, etc.). Tagged by <c>template_id</c> because
    /// the cardinality is bounded by the curated <see cref="ValidatingSbeDispatcher.KnownTemplateIds"/>
    /// set (~30 entries) and per-template visibility is operationally useful.
    /// </summary>
    public static readonly Counter<long> MalformedKnownTemplate =
        Meter.CreateCounter<long>("umdf.sbe.malformed_known_template");
}
