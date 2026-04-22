namespace B3.Umdf.Book;

/// <summary>
/// Per-security 1-second OHLCV candle aggregator with 10-hour retention.
/// Features:
/// <list type="bullet">
///   <item>Gap-fill: seconds without trades produce flat candles (OHLC = prev.close, volume = 0), capped at 60s per gap.</item>
///   <item>Continuity: each new candle's open = previous candle's close.</item>
///   <item>Fixed retention: keeps only the most recent 10 hours (36,000 1s candles).</item>
///   <item>Chunked storage: candles live in fixed-size chunks of <see cref="ChunkSize"/> entries
///         (24 KB each, well below the LOH threshold). Chunks are allocated on demand, so idle
///         securities use ~1 KB each, while active ones grow one chunk at a time without ever
///         producing large LOH allocations or copy-on-grow bursts.</item>
/// </list>
/// Resolution is always 1 second — the frontend aggregates to coarser resolutions as needed.
/// Thread-safety: all mutations happen on the owning group's single thread.
/// Snapshot reads (<see cref="GetCandles"/>, <see cref="GetLatest"/>) may run from another
/// thread and use a version-stamp spin-copy for consistency.
/// </summary>
internal sealed class CandleAggregator
{
    private const int MaxGapFill = 60; // max gap-fill candles per trade gap
    internal const int MaxRetainedCandles = 10 * 60 * 60; // 10h @ 1s resolution
    /// <summary>
    /// 512 candles × 56 bytes = 28,672 bytes per chunk — comfortably below the .NET LOH
    /// threshold (85,000 bytes), so chunk allocations stay on the SOH and are cheap to GC.
    /// </summary>
    internal const int ChunkSize = 512;
    internal static readonly int MaxChunks = (MaxRetainedCandles + ChunkSize - 1) / ChunkSize;

    /// <summary>
    /// Fixed-length array of chunk references. Allocated once at construction (small: ~MaxChunks
    /// pointers ≈ 568 bytes per security). Individual chunks are allocated lazily on first write.
    /// </summary>
    private readonly Candle[]?[] _chunks = new Candle[MaxChunks][];
    private long _lastClose;
    /// <summary>Logical index of the oldest candle (0..MaxRetainedCandles-1).</summary>
    private volatile int _head;
    private volatile int _count;
    private volatile int _version; // bumped on every mutation

    /// <summary>Resolution is always 1 second.</summary>
    public int Resolution => 1;

    /// <summary>Number of candles stored.</summary>
    public int Count => _count;

    /// <summary>Version counter — incremented on every Add.</summary>
    public int Version => _version;

    /// <summary>Convenience overload for tests/callers that don't have a session VWAP.
    /// Stamps the trade price as Avg (best-effort fallback).</summary>
    public bool Add(long price, long quantity, long timestampSeconds)
        => Add(price, quantity, timestampSeconds, price);

    /// <summary>
    /// Add a trade to the aggregator. Handles gap-fill and continuity.
    /// <paramref name="sessionVwap"/> is B3's authoritative session VWAP at the moment of
    /// the trade (InfoSnapshot.VwapPrice); stamped on the candle's Avg field. Pass the
    /// trade price if no VWAP has been published yet — a sane fallback.
    /// Returns true if a new candle was started (vs updating existing).
    /// </summary>
    public bool Add(long price, long quantity, long timestampSeconds, long sessionVwap)
    {
        if (_count > 0)
        {
            int lastPos = (_head + _count - 1) % MaxRetainedCandles;
            ref var last = ref _chunks[lastPos / ChunkSize]![lastPos % ChunkSize];
            if (timestampSeconds < last.Time)
                return false; // out-of-order trade — ignore to preserve time ordering

            if (timestampSeconds == last.Time)
            {
                // Same second — update in place
                if (price > last.High) last.High = price;
                if (price < last.Low) last.Low = price;
                last.Close = price;
                last.Volume += quantity;
                last.Avg = sessionVwap;
                _lastClose = price;
                _version++;
                return false;
            }

            // Gap-fill: insert flat candles for empty seconds between last and current.
            // Avg = previous bucket's VWAP (no trades happened during the gap, so VWAP is unchanged).
            long gapStart = last.Time + 1;
            int gaps = 0;
            long gapAvg = last.Avg;
            while (gapStart < timestampSeconds && gaps < MaxGapFill)
            {
                AppendCandle(new Candle(gapStart, _lastClose, _lastClose, _lastClose, _lastClose, 0, gapAvg));
                gapStart++;
                gaps++;
            }

            AppendCandle(new Candle(timestampSeconds, _lastClose, Math.Max(price, _lastClose), Math.Min(price, _lastClose), price, quantity, sessionVwap));
        }
        else
        {
            // First candle ever
            AppendCandle(new Candle(timestampSeconds, price, price, price, price, quantity, sessionVwap));
        }

        _lastClose = price;
        _version++;
        return true;
    }

    /// <summary>
    /// Get a snapshot of all candles (thread-safe via version-stamp spin-copy).
    /// </summary>
    public Candle[] GetCandles()
    {
        while (true)
        {
            int v1 = _version;
            int head = _head;
            int count = _count;
            var snapshot = new Candle[count];
            for (int i = 0; i < count; i++)
            {
                int pos = (head + i) % MaxRetainedCandles;
                var chunk = _chunks[pos / ChunkSize];
                if (chunk is null)
                {
                    // A chunk we expected to read has not been allocated yet — possible only if
                    // the writer is mid-allocation concurrently. Bail out and retry; the version
                    // check would also catch it but this avoids dereferencing null.
                    snapshot = Array.Empty<Candle>();
                    break;
                }
                snapshot[i] = chunk[pos % ChunkSize];
            }
            int v2 = _version;
            if (v1 == v2 && (count == 0 || snapshot.Length == count)) return snapshot;
        }
    }

    /// <summary>
    /// Get the latest candle, or null if no data.
    /// </summary>
    public Candle? GetLatest()
    {
        while (true)
        {
            int v1 = _version;
            int count = _count;
            if (count == 0) return null;
            int pos = (_head + count - 1) % MaxRetainedCandles;
            var chunk = _chunks[pos / ChunkSize];
            if (chunk is null)
            {
                // Mid-allocation race; retry.
                continue;
            }
            var candle = chunk[pos % ChunkSize];
            int v2 = _version;
            if (v1 == v2) return candle;
        }
    }

    private void AppendCandle(in Candle candle)
    {
        int writePos;
        if (_count == MaxRetainedCandles)
        {
            // At the retention cap: ring-buffer write; advance head.
            writePos = (_head + _count) % MaxRetainedCandles;
            _chunks[writePos / ChunkSize]![writePos % ChunkSize] = candle;
            _head = (_head + 1) % MaxRetainedCandles;
            return;
        }

        writePos = (_head + _count) % MaxRetainedCandles;
        int chunkIdx = writePos / ChunkSize;
        var chunk = _chunks[chunkIdx];
        if (chunk is null)
        {
            // Lazily allocate the chunk we are about to write into. One allocation of 24 KB,
            // never reallocated or copied — no realloc bursts, no LOH pressure.
            chunk = new Candle[ChunkSize];
            _chunks[chunkIdx] = chunk;
        }
        chunk[writePos % ChunkSize] = candle;
        _count++;
    }
}

/// <summary>
/// OHLCV candle data point. <see cref="Avg"/> is the volume-weighted average price
/// (sum(price × qty) / sum(qty)) computed from all trades that landed in this
/// 1-second bucket; for gap-fill candles it equals <see cref="Close"/>.
/// </summary>
internal record struct Candle(long Time, long Open, long High, long Low, long Close, long Volume, long Avg);
