using System.Runtime.InteropServices;

namespace B3.Umdf.Server;

/// <summary>
/// Per-security 1-second OHLCV candle aggregator (unbounded history).
/// Features:
/// <list type="bullet">
///   <item>Gap-fill: seconds without trades produce flat candles (OHLC = prev.close, volume = 0), capped at 60s per gap.</item>
///   <item>Continuity: each new candle's open = previous candle's close.</item>
/// </list>
/// Resolution is always 1 second — the frontend aggregates to coarser resolutions as needed.
/// Thread-safety: all mutations happen on the owning group's single thread.
/// Snapshot reads (GetCandles) may run from another thread and use a simple spin-copy for consistency.
/// </summary>
internal sealed class CandleAggregator
{
    private const int MaxGapFill = 60; // max gap-fill candles per trade gap

    private readonly List<Candle> _candles = new();
    private long _lastClose;
    private volatile int _version; // bumped on every mutation

    /// <summary>Resolution is always 1 second.</summary>
    public int Resolution => 1;

    /// <summary>Number of candles stored.</summary>
    public int Count => _candles.Count;

    /// <summary>Version counter — incremented on every Add.</summary>
    public int Version => _version;

    /// <summary>
    /// Add a trade to the aggregator. Handles gap-fill and continuity.
    /// Returns true if a new candle was started (vs updating existing).
    /// </summary>
    public bool Add(long price, long quantity, long timestampSeconds)
    {
        if (_candles.Count > 0)
        {
            ref var last = ref CollectionsMarshal.AsSpan(_candles)[^1];
            if (timestampSeconds == last.Time)
            {
                // Same second — update in place
                if (price > last.High) last.High = price;
                if (price < last.Low) last.Low = price;
                last.Close = price;
                last.Volume += quantity;
                _lastClose = price;
                _version++;
                return false;
            }

            // Gap-fill: insert flat candles for empty seconds between last and current
            long gapStart = last.Time + 1;
            int gaps = 0;
            while (gapStart < timestampSeconds && gaps < MaxGapFill)
            {
                _candles.Add(new Candle(gapStart, _lastClose, _lastClose, _lastClose, _lastClose, 0));
                gapStart++;
                gaps++;
            }

            // New candle with open = previous close (continuity)
            _candles.Add(new Candle(timestampSeconds, _lastClose, Math.Max(price, _lastClose), Math.Min(price, _lastClose), price, quantity));
        }
        else
        {
            // First candle ever
            _candles.Add(new Candle(timestampSeconds, price, price, price, price, quantity));
        }

        _lastClose = price;
        _version++;
        return true;
    }

    /// <summary>
    /// Get a snapshot of all candles (thread-safe via spin-copy).
    /// </summary>
    public Candle[] GetCandles()
    {
        while (true)
        {
            int v1 = _version;
            var snapshot = _candles.ToArray();
            int v2 = _version;
            if (v1 == v2) return snapshot;
        }
    }

    /// <summary>
    /// Get the latest candle, or null if no data.
    /// </summary>
    public Candle? GetLatest()
    {
        if (_candles.Count == 0) return null;
        return _candles[^1];
    }
}

/// <summary>OHLCV candle data point.</summary>
internal record struct Candle(long Time, long Open, long High, long Low, long Close, long Volume);
