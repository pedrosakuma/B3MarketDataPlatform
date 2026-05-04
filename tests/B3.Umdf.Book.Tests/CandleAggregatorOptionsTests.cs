using System;
using B3.Umdf.Book;
using Xunit;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Stress / option-coverage tests for <see cref="CandleAggregator"/> driven through
/// <see cref="CandleAggregatorOptions"/>. Validates that gap-fill cap, retention
/// rollover, bucket-size bucketing, and out-of-order trade handling honor the
/// configured options and that the bust-aware APIs (<c>FindBucketIndex</c>,
/// <c>ReplaceAt</c>, <c>IncrementBustedCount</c>) co-exist cleanly.
/// </summary>
public class CandleAggregatorOptionsTests
{
    [Fact]
    public void Defaults_MatchLegacyConstants()
    {
        var opts = CandleAggregatorOptions.Default;
        Assert.Equal(60, opts.MaxGapFill);
        Assert.Equal(CandleAggregator.MaxRetainedCandles, opts.RetentionWindow);
        Assert.Equal(1, opts.BucketSize);

        var agg = new CandleAggregator();
        Assert.Equal(60, agg.MaxGapFill);
        Assert.Equal(CandleAggregator.MaxRetainedCandles, agg.RetentionWindow);
        Assert.Equal(1, agg.Resolution);
    }

    [Fact]
    public void GapLongerThanMaxGapFill_StopsFillingAndJumpsToTradeBucket()
    {
        // Custom small gap-fill cap (3) — easier to reason about than the default 60.
        var opts = new CandleAggregatorOptions { MaxGapFill = 3, RetentionWindow = 100 };
        var agg = new CandleAggregator(opts);

        agg.Add(price: 100, quantity: 1, timestampSeconds: 1_000);
        // Gap of 10 seconds — well past MaxGapFill=3.
        agg.Add(price: 110, quantity: 2, timestampSeconds: 1_010);

        // Expect: bucket 1000 (real), 1001/1002/1003 (3 flat fillers), 1010 (real). 5 candles total.
        var candles = agg.GetCandles();
        Assert.Equal(5, candles.Length);
        Assert.Equal(1_000, candles[0].Time);
        Assert.Equal(1_001, candles[1].Time); Assert.Equal(0, candles[1].Volume);
        Assert.Equal(1_002, candles[2].Time); Assert.Equal(0, candles[2].Volume);
        Assert.Equal(1_003, candles[3].Time); Assert.Equal(0, candles[3].Volume);
        Assert.Equal(1_010, candles[4].Time); Assert.Equal(2, candles[4].Volume);
    }

    [Fact]
    public void GapExactlyAtMaxGapFill_FillsExactlyMaxGapFillBuckets()
    {
        // Trade at t and trade at t + (MaxGapFill+1) → MaxGapFill flat candles between.
        var opts = new CandleAggregatorOptions { MaxGapFill = 5, RetentionWindow = 50 };
        var agg = new CandleAggregator(opts);
        agg.Add(100, 1, 0);
        agg.Add(110, 1, 6);
        var candles = agg.GetCandles();
        // bucket 0, 1..5 (5 fillers), 6 → 7 total
        Assert.Equal(7, candles.Length);
        for (int i = 1; i <= 5; i++) Assert.Equal(0, candles[i].Volume);
    }

    [Fact]
    public void RetentionRollover_EvictsOldestBeyondConfiguredWindow()
    {
        const int retention = 32; // tiny window for fast assertion
        var opts = new CandleAggregatorOptions { MaxGapFill = 0, RetentionWindow = retention };
        var agg = new CandleAggregator(opts);

        const int pushed = retention + 17;
        for (int i = 0; i < pushed; i++)
            agg.Add(price: 100 + i, quantity: 1, timestampSeconds: i);

        Assert.Equal(retention, agg.Count);
        var candles = agg.GetCandles();
        Assert.Equal(retention, candles.Length);
        // Oldest 17 candles should have been evicted; first stored candle is at t = pushed - retention.
        Assert.Equal(pushed - retention, candles[0].Time);
        Assert.Equal(pushed - 1, candles[^1].Time);
    }

    [Fact]
    public void OutOfOrderBurst_LateTradesIntoCurrentBucket_AreIgnored()
    {
        var opts = new CandleAggregatorOptions { MaxGapFill = 5, RetentionWindow = 100 };
        var agg = new CandleAggregator(opts);
        agg.Add(100, 1, 1_000);
        agg.Add(105, 1, 1_001);
        // Burst of out-of-order trades (timestamps strictly behind last bucket).
        for (int i = 0; i < 20; i++)
        {
            agg.Add(price: 50, quantity: 100, timestampSeconds: 999 - i);
        }
        // Aggregator should still hold exactly 2 candles.
        Assert.Equal(2, agg.Count);
        var candles = agg.GetCandles();
        Assert.Equal(1_000, candles[0].Time);
        Assert.Equal(1_001, candles[1].Time);
        // Volumes unchanged by the late burst.
        Assert.Equal(1, candles[0].Volume);
        Assert.Equal(1, candles[1].Volume);
    }

    [Fact]
    public void BucketSize_GreaterThanOne_FloorsTimestampsAndStepsGapFill()
    {
        var opts = new CandleAggregatorOptions { MaxGapFill = 10, RetentionWindow = 100, BucketSize = 5 };
        var agg = new CandleAggregator(opts);

        Assert.Equal(5, agg.Resolution);

        // Three trades inside bucket [1000, 1005).
        agg.Add(100, 2, 1_000);
        agg.Add(110, 3, 1_002);
        agg.Add(105, 1, 1_004);
        // Gap to bucket [1015, 1020) — two empty buckets in between.
        agg.Add(120, 1, 1_017);

        var c = agg.GetCandles();
        Assert.Equal(4, c.Length);
        Assert.Equal(1_000, c[0].Time);
        Assert.Equal(6, c[0].Volume);  // 2+3+1
        Assert.Equal(110, c[0].High);
        Assert.Equal(100, c[0].Open);
        Assert.Equal(105, c[0].Close);
        Assert.Equal(1_005, c[1].Time); Assert.Equal(0, c[1].Volume);
        Assert.Equal(1_010, c[2].Time); Assert.Equal(0, c[2].Volume);
        Assert.Equal(1_015, c[3].Time); Assert.Equal(1, c[3].Volume);
    }

    [Fact]
    public void BustAwareApis_CoexistWithCustomOptions()
    {
        var opts = new CandleAggregatorOptions { MaxGapFill = 5, RetentionWindow = 50, BucketSize = 1 };
        var agg = new CandleAggregator(opts);
        agg.Add(100, 5, 1_000);
        agg.Add(105, 3, 1_001);

        int idx = agg.FindBucketIndex(1_000);
        Assert.Equal(0, idx);
        var orig = agg.GetAt(idx);

        // Replace the bucket with a recomputed candle and verify it sticks.
        agg.ReplaceAt(idx, new Candle(orig.Time, 90, 95, 85, 92, 4, orig.Avg));
        Assert.Equal(92, agg.GetAt(idx).Close);

        agg.IncrementBustedCount(1_001);
        Assert.Equal(1, agg.GetBustedCount(1_001));

        Assert.True(agg.RemoveLast());
        Assert.Equal(1, agg.Count);
    }

    [Fact]
    public void BustAwareApis_FindBucketIndex_RespectsBucketSize()
    {
        var opts = new CandleAggregatorOptions { MaxGapFill = 2, RetentionWindow = 100, BucketSize = 5 };
        var agg = new CandleAggregator(opts);
        agg.Add(100, 1, 1_000);
        agg.Add(110, 1, 1_005);

        // Any timestamp inside the [1000, 1005) bucket maps to logical index 0.
        Assert.Equal(0, agg.FindBucketIndex(1_000));
        Assert.Equal(0, agg.FindBucketIndex(1_004));
        Assert.Equal(1, agg.FindBucketIndex(1_005));
        Assert.Equal(1, agg.FindBucketIndex(1_009));
        Assert.Equal(-1, agg.FindBucketIndex(1_010)); // not yet stored

        agg.IncrementBustedCount(1_007); // floors to 1_005
        Assert.Equal(1, agg.GetBustedCount(1_005));
        Assert.Equal(1, agg.GetBustedCount(1_009));
    }

    [Fact]
    public void Options_Validate_RejectsNonPositiveValues()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new CandleAggregator(new CandleAggregatorOptions { RetentionWindow = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new CandleAggregator(new CandleAggregatorOptions { BucketSize = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new CandleAggregator(new CandleAggregatorOptions { MaxGapFill = -1 }));
    }
}
