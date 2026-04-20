namespace B3.Umdf.Server.Tests;

public class CandleAggregatorTests
{
    [Fact]
    public void FirstAdd_CreatesCandle_WithOpenEqualToPrice()
    {
        var agg = new CandleAggregator();
        bool isNew = agg.Add(price: 1000, quantity: 10, timestampSeconds: 100);

        Assert.True(isNew);
        Assert.Equal(1, agg.Count);

        var c = agg.GetLatest()!.Value;
        Assert.Equal(100L, c.Time);
        Assert.Equal(1000L, c.Open);
        Assert.Equal(1000L, c.High);
        Assert.Equal(1000L, c.Low);
        Assert.Equal(1000L, c.Close);
        Assert.Equal(10L, c.Volume);
    }

    [Fact]
    public void SameSecond_UpdatesInPlace_DoesNotCreateNewCandle()
    {
        var agg = new CandleAggregator();
        agg.Add(price: 1000, quantity: 10, timestampSeconds: 100);
        bool isNew = agg.Add(price: 1100, quantity: 5, timestampSeconds: 100);

        Assert.False(isNew);
        Assert.Equal(1, agg.Count);

        var c = agg.GetLatest()!.Value;
        Assert.Equal(1000L, c.Open);   // open stays at first trade
        Assert.Equal(1100L, c.High);   // new high
        Assert.Equal(1000L, c.Low);
        Assert.Equal(1100L, c.Close);  // close moves to last trade
        Assert.Equal(15L, c.Volume);   // volume accumulates
    }

    [Fact]
    public void NewSecond_OpenEqualsClosePreviousCandle()
    {
        var agg = new CandleAggregator();
        agg.Add(price: 1000, quantity: 10, timestampSeconds: 100);
        agg.Add(price: 950, quantity: 5, timestampSeconds: 100); // close = 950
        agg.Add(price: 1020, quantity: 3, timestampSeconds: 101);

        Assert.Equal(2, agg.Count);
        var c = agg.GetLatest()!.Value;
        Assert.Equal(101L, c.Time);
        Assert.Equal(950L, c.Open);   // continuity: open = prev close (950)
        Assert.Equal(1020L, c.High);
        Assert.Equal(950L, c.Low);    // open < price, so low = open = 950
        Assert.Equal(1020L, c.Close);
    }

    [Fact]
    public void Gap_InsertsGapFillCandles_CappedAt60()
    {
        var agg = new CandleAggregator();
        agg.Add(price: 1000, quantity: 10, timestampSeconds: 100);
        // Jump 62 seconds ahead — should produce 60 gap-fill candles, not 61
        agg.Add(price: 1100, quantity: 5, timestampSeconds: 162);

        // 1 original + 60 gap-fill + 1 new = 62
        Assert.Equal(62, agg.Count);

        // Gap-fill candles should be flat at last close (1000)
        var candles = agg.GetCandles();
        for (int i = 1; i <= 60; i++)
        {
            var gf = candles[i];
            Assert.Equal(100L + i, gf.Time);
            Assert.Equal(1000L, gf.Open);
            Assert.Equal(1000L, gf.Close);
            Assert.Equal(0L, gf.Volume);
        }
    }

    [Fact]
    public void SmallGap_InsertExactNumberOfGapFillCandles()
    {
        var agg = new CandleAggregator();
        agg.Add(price: 500, quantity: 1, timestampSeconds: 10);
        agg.Add(price: 600, quantity: 1, timestampSeconds: 13); // gap: t=11, t=12

        Assert.Equal(4, agg.Count); // 1 + 2 gap-fill + 1 new
        var candles = agg.GetCandles();
        Assert.Equal(11L, candles[1].Time);
        Assert.Equal(12L, candles[2].Time);
        Assert.Equal(13L, candles[3].Time);
    }

    [Fact]
    public void Version_IncreasesOnEveryAdd()
    {
        var agg = new CandleAggregator();
        int v0 = agg.Version;
        agg.Add(price: 100, quantity: 1, timestampSeconds: 1);
        int v1 = agg.Version;
        agg.Add(price: 110, quantity: 1, timestampSeconds: 1); // same second
        int v2 = agg.Version;
        agg.Add(price: 120, quantity: 1, timestampSeconds: 2); // new second
        int v3 = agg.Version;

        Assert.True(v1 > v0);
        Assert.True(v2 > v1);
        Assert.True(v3 > v2);
    }

    [Fact]
    public void GetCandles_ReturnsSnapshot_SameAsCount()
    {
        var agg = new CandleAggregator();
        agg.Add(price: 100, quantity: 1, timestampSeconds: 1);
        agg.Add(price: 200, quantity: 2, timestampSeconds: 2);
        agg.Add(price: 300, quantity: 3, timestampSeconds: 3);

        var candles = agg.GetCandles();
        Assert.Equal(3, candles.Length);
        Assert.Equal(1L, candles[0].Time);
        Assert.Equal(2L, candles[1].Time);
        Assert.Equal(3L, candles[2].Time);
    }

    [Fact]
    public void GetLatest_ReturnsNull_WhenEmpty()
    {
        var agg = new CandleAggregator();
        Assert.Null(agg.GetLatest());
    }

    [Fact]
    public void GetCandles_Empty_ReturnsEmptyArray()
    {
        var agg = new CandleAggregator();
        Assert.Empty(agg.GetCandles());
    }

    [Fact]
    public void NewSecond_HighIsMaxOfOpenAndPrice()
    {
        var agg = new CandleAggregator();
        agg.Add(price: 1000, quantity: 1, timestampSeconds: 1); // close = 1000
        // Next candle: open = 1000, price = 800 → high = max(1000, 800) = 1000, low = 800
        agg.Add(price: 800, quantity: 1, timestampSeconds: 2);

        var c = agg.GetLatest()!.Value;
        Assert.Equal(1000L, c.Open);
        Assert.Equal(1000L, c.High);  // open was the high
        Assert.Equal(800L, c.Low);
        Assert.Equal(800L, c.Close);
    }

    [Fact]
    public void OutOfOrder_TradeIsIgnored_NoNewCandleCreated()
    {
        var agg = new CandleAggregator();
        agg.Add(price: 1000, quantity: 10, timestampSeconds: 100);
        agg.Add(price: 1100, quantity: 5, timestampSeconds: 101);

        bool added = agg.Add(price: 500, quantity: 99, timestampSeconds: 99); // older than t=100

        Assert.False(added);
        Assert.Equal(2, agg.Count); // unchanged
        Assert.Equal(1100L, agg.GetLatest()!.Value.Close); // last candle unaffected
    }

    [Fact]
    public void Retention_KeepsOnlyMostRecentTenHours()
    {
        var agg = new CandleAggregator();

        for (int i = 0; i < CandleAggregator.MaxRetainedCandles + 10; i++)
            agg.Add(price: 1000 + i, quantity: 1, timestampSeconds: i);

        var candles = agg.GetCandles();
        Assert.Equal(CandleAggregator.MaxRetainedCandles, agg.Count);
        Assert.Equal(CandleAggregator.MaxRetainedCandles, candles.Length);
        Assert.Equal(10L, candles[0].Time);
        Assert.Equal(CandleAggregator.MaxRetainedCandles + 9L, candles[^1].Time);
    }
}
