namespace B3.Umdf.Book;

/// <summary>
/// Immutable configuration for <see cref="CandleAggregator"/>. Exposes the previously
/// hard-coded gap-fill cap, retention window, and bucket size so deployments and
/// tests can tune the aggregator without recompiling.
///
/// <para>Defaults match the historical hard-coded values:
/// <c>MaxGapFill=60</c>, <c>RetentionWindow=36 000</c> (10 hours of 1-second buckets),
/// <c>BucketSize=1</c>.</para>
/// </summary>
internal sealed class CandleAggregatorOptions
{
    /// <summary>Default values matching the legacy hard-coded behavior.</summary>
    public static CandleAggregatorOptions Default { get; } = new();

    /// <summary>
    /// Maximum number of flat gap-fill candles inserted between two trades. Trades
    /// arriving more than <c>MaxGapFill * BucketSize</c> seconds apart skip the
    /// flat-fill and only emit the bucket containing the new trade.
    /// </summary>
    public int MaxGapFill { get; init; } = 60;

    /// <summary>
    /// Maximum number of candles retained per security (ring-buffer capacity). Once
    /// this many candles are stored, the oldest is evicted on each new append.
    /// Default of 36 000 buckets at <c>BucketSize=1</c> covers a 10-hour session.
    /// </summary>
    public int RetentionWindow { get; init; } = 10 * 60 * 60;

    /// <summary>
    /// Bucket width in seconds. Trade timestamps are floored to a multiple of this
    /// value before bucketing; gap-fill steps in increments of this value.
    /// Defaults to 1-second buckets.
    /// </summary>
    public int BucketSize { get; init; } = 1;

    internal void Validate()
    {
        if (MaxGapFill < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxGapFill), MaxGapFill, "must be >= 0");
        if (RetentionWindow <= 0)
            throw new ArgumentOutOfRangeException(nameof(RetentionWindow), RetentionWindow, "must be > 0");
        if (BucketSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(BucketSize), BucketSize, "must be > 0");
    }
}
