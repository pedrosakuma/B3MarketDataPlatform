namespace B3.Umdf.Book;

/// <summary>
/// Immutable configuration for <see cref="NewsReassembler"/>. Surfaces the
/// previously hard-coded TTL, aggregate inflight byte budget, per-part byte
/// cap, and per-assembly part-count cap so deployments and tests can tune the
/// reassembler without recompiling.
///
/// <para>Defaults match the historical hard-coded values:
/// <c>Ttl = 5s</c>, <c>MaxInflightBytes = 16 MiB</c>,
/// <c>MaxPartBytes = 16 MiB</c> (i.e. effectively unbounded — a single part
/// could already fill the inflight budget under the legacy code),
/// <c>MaxParts = 64</c>.</para>
/// </summary>
internal sealed class NewsReassemblerOptions
{
    /// <summary>Default values matching the legacy hard-coded behavior.</summary>
    public static NewsReassemblerOptions Default { get; } = new();

    /// <summary>
    /// Maximum age of an in-flight assembly before it is dropped. Sweep happens on
    /// each <c>Submit</c>. Default 5 s.
    /// </summary>
    public TimeSpan Ttl { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Aggregate buffered-bytes budget across all in-flight assemblies. New
    /// assemblies trigger LRU eviction once the cap is exceeded. Default 16 MiB.
    /// </summary>
    public long MaxInflightBytes { get; init; } = 16L * 1024 * 1024;

    /// <summary>
    /// Maximum number of bytes any single part may contribute (sum of headline +
    /// text + url payload of one part). Parts exceeding this cap drop the entire
    /// assembly. Default 16 MiB (matches <see cref="MaxInflightBytes"/> so the
    /// historical behavior — no per-part validation — is preserved).
    /// </summary>
    public long MaxPartBytes { get; init; } = 16L * 1024 * 1024;

    /// <summary>
    /// Maximum number of parts a single assembly may declare via the SBE
    /// <c>PartCount</c> field. Larger declarations are dropped. Default 64.
    /// </summary>
    public int MaxParts { get; init; } = 64;

    internal void Validate()
    {
        if (Ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(Ttl), Ttl, "must be > 0");
        if (MaxInflightBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxInflightBytes), MaxInflightBytes, "must be > 0");
        if (MaxPartBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxPartBytes), MaxPartBytes, "must be > 0");
        if (MaxParts <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxParts), MaxParts, "must be > 0");
    }
}
