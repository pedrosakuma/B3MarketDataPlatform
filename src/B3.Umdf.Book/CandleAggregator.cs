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
    /// <summary>Default value of <see cref="CandleAggregatorOptions.RetentionWindow"/>; preserved as a const for back-compat with existing callers/tests.</summary>
    internal const int MaxRetainedCandles = 10 * 60 * 60; // 10h @ 1s resolution
    /// <summary>
    /// 512 candles × 56 bytes = 28,672 bytes per chunk — comfortably below the .NET LOH
    /// threshold (85,000 bytes), so chunk allocations stay on the SOH and are cheap to GC.
    /// </summary>
    internal const int ChunkSize = 512;
    /// <summary>Number of chunk slots required for the default retention. Instance retention may differ; see <see cref="_maxChunks"/>.</summary>
    internal static readonly int MaxChunks = (MaxRetainedCandles + ChunkSize - 1) / ChunkSize;

    private readonly int _maxGapFill;
    private readonly int _maxRetainedCandles;
    private readonly int _bucketSize;
    private readonly int _maxChunks;

    /// <summary>
    /// Fixed-length array of chunk references. Allocated once at construction (small: ~MaxChunks
    /// pointers ≈ 568 bytes per security). Individual chunks are allocated lazily on first write.
    /// </summary>
    private readonly Candle[]?[] _chunks;
    private long _lastClose;
    /// <summary>Logical index of the oldest candle (0..MaxRetainedCandles-1).</summary>
    private volatile int _head;
    private volatile int _count;
    private volatile int _version; // bumped on every mutation

    // Per-bucket count of busted trades that fell outside the candle-reversal
    // window (e.g. the bucket's contributing trades were already evicted from
    // the ring, so OHLCV cannot be exactly recomputed). Exposed via
    // <see cref="GetBustedCount"/> so dashboards can flag the affected candle
    // as "stat-quality-degraded" without distorting OHLC.
    private Dictionary<long, int>? _bustedCounts;

    /// <summary>Resolution in seconds — equals the configured bucket size.</summary>
    public int Resolution => _bucketSize;

    /// <summary>Maximum number of candles this instance will retain (configurable).</summary>
    public int RetentionWindow => _maxRetainedCandles;

    /// <summary>Maximum gap-fill candles inserted between consecutive trades (configurable).</summary>
    public int MaxGapFill => _maxGapFill;

    /// <summary>Number of candles stored.</summary>
    public int Count => _count;

    /// <summary>Version counter — incremented on every Add.</summary>
    public int Version => _version;

    /// <summary>Most recent close price written to the aggregator, or 0 if empty.</summary>
    public long LastClose => _lastClose;

    /// <summary>Construct with default options (matches legacy behavior: 60s gap-fill cap, 10h retention, 1s buckets).</summary>
    public CandleAggregator() : this(CandleAggregatorOptions.Default) { }

    /// <summary>Construct with explicit options.</summary>
    public CandleAggregator(CandleAggregatorOptions options)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        options.Validate();
        _maxGapFill = options.MaxGapFill;
        _maxRetainedCandles = options.RetentionWindow;
        _bucketSize = options.BucketSize;
        _maxChunks = (_maxRetainedCandles + ChunkSize - 1) / ChunkSize;
        _chunks = new Candle[_maxChunks][];
    }

    /// <summary>
    /// Locate the candle whose <see cref="Candle.Time"/> equals
    /// <paramref name="timestampSeconds"/>. Returns the logical index
    /// (0 = oldest retained) or -1 if no such candle exists in the
    /// retention window. Buckets are 1-second contiguous (gap-fill
    /// inserts placeholder candles) so the lookup is O(1).
    /// </summary>
    public int FindBucketIndex(long timestampSeconds)
    {
        int count = _count;
        if (count == 0) return -1;
        int oldestPos = _head;
        var oldestChunk = _chunks[oldestPos / ChunkSize];
        if (oldestChunk is null) return -1;
        long oldestTime = oldestChunk[oldestPos % ChunkSize].Time;
        long bucket = BucketFloor(timestampSeconds);
        long delta = bucket - oldestTime;
        if (delta < 0) return -1;
        long step = delta / _bucketSize;
        if (step >= count) return -1;
        return (int)step;
    }

    /// <summary>Read the candle at logical index <paramref name="logicalIndex"/>.</summary>
    public Candle GetAt(int logicalIndex)
    {
        if ((uint)logicalIndex >= (uint)_count)
            throw new ArgumentOutOfRangeException(nameof(logicalIndex));
        int pos = (_head + logicalIndex) % _maxRetainedCandles;
        return _chunks[pos / ChunkSize]![pos % ChunkSize];
    }

    /// <summary>
    /// Overwrite the candle at <paramref name="logicalIndex"/>. Used by the
    /// trade-bust reversal path to swap a recomputed OHLCV in for a
    /// busted bucket. If the replaced candle is the most recent one,
    /// <see cref="LastClose"/> is updated to the new close.
    /// </summary>
    public void ReplaceAt(int logicalIndex, in Candle candle)
    {
        if ((uint)logicalIndex >= (uint)_count)
            throw new ArgumentOutOfRangeException(nameof(logicalIndex));
        int pos = (_head + logicalIndex) % _maxRetainedCandles;
        _chunks[pos / ChunkSize]![pos % ChunkSize] = candle;
        if (logicalIndex == _count - 1)
            _lastClose = candle.Close;
        _version++;
    }

    /// <summary>
    /// Drop the most recent candle. Used by trade-bust reversal when the
    /// busted trade was the only contributor to the latest bucket. Falls
    /// back <see cref="LastClose"/> to the now-last bucket's close (or 0
    /// if the aggregator is empty).
    /// </summary>
    public bool RemoveLast()
    {
        if (_count == 0) return false;
        _count--;
        if (_count > 0)
        {
            int pos = (_head + _count - 1) % _maxRetainedCandles;
            _lastClose = _chunks[pos / ChunkSize]![pos % ChunkSize].Close;
        }
        else
        {
            _lastClose = 0;
        }
        _version++;
        return true;
    }

    /// <summary>
    /// Annotate a candle bucket as containing a busted trade we could not
    /// fully reverse (typically because its sibling trades fell out of the
    /// recent-trades retention window). OHLCV is left untouched — the
    /// counter is purely advisory and surfaces via <see cref="GetBustedCount"/>.
    /// </summary>
    public void IncrementBustedCount(long timestampSeconds)
    {
        long bucket = BucketFloor(timestampSeconds);
        _bustedCounts ??= new Dictionary<long, int>();
        _bustedCounts.TryGetValue(bucket, out var c);
        _bustedCounts[bucket] = c + 1;
        _version++;
    }

    /// <summary>How many busted trades fall in the bucket at <paramref name="timestampSeconds"/>.</summary>
    public int GetBustedCount(long timestampSeconds)
        => _bustedCounts is { } b && b.TryGetValue(BucketFloor(timestampSeconds), out var c) ? c : 0;

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
        long bucket = BucketFloor(timestampSeconds);
        if (_count > 0)
        {
            int lastPos = (_head + _count - 1) % _maxRetainedCandles;
            ref var last = ref _chunks[lastPos / ChunkSize]![lastPos % ChunkSize];
            if (bucket < last.Time)
                return false; // out-of-order trade — ignore to preserve time ordering

            if (bucket == last.Time)
            {
                // Same bucket — update in place
                if (price > last.High) last.High = price;
                if (price < last.Low) last.Low = price;
                last.Close = price;
                last.Volume += quantity;
                last.Avg = sessionVwap;
                _lastClose = price;
                _version++;
                return false;
            }

            // Gap-fill: insert flat candles for empty buckets between last and current.
            // Avg = previous bucket's VWAP (no trades happened during the gap, so VWAP is unchanged).
            long gapStart = last.Time + _bucketSize;
            int gaps = 0;
            long gapAvg = last.Avg;
            while (gapStart < bucket && gaps < _maxGapFill)
            {
                AppendCandle(new Candle(gapStart, _lastClose, _lastClose, _lastClose, _lastClose, 0, gapAvg));
                gapStart += _bucketSize;
                gaps++;
            }

            AppendCandle(new Candle(bucket, _lastClose, Math.Max(price, _lastClose), Math.Min(price, _lastClose), price, quantity, sessionVwap));
        }
        else
        {
            // First candle ever
            AppendCandle(new Candle(bucket, price, price, price, price, quantity, sessionVwap));
        }

        _lastClose = price;
        _version++;
        return true;
    }

    private long BucketFloor(long timestampSeconds)
    {
        if (_bucketSize == 1) return timestampSeconds;
        // Floor toward negative infinity; trade timestamps are >= 0 in practice but
        // guard for safety against synthetic test inputs.
        long b = timestampSeconds / _bucketSize;
        if (timestampSeconds < 0 && timestampSeconds % _bucketSize != 0) b--;
        return b * _bucketSize;
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
                int pos = (head + i) % _maxRetainedCandles;
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
            int pos = (_head + count - 1) % _maxRetainedCandles;
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
        if (_count == _maxRetainedCandles)
        {
            // At the retention cap: ring-buffer write; advance head.
            writePos = (_head + _count) % _maxRetainedCandles;
            _chunks[writePos / ChunkSize]![writePos % ChunkSize] = candle;
            _head = (_head + 1) % _maxRetainedCandles;
            return;
        }

        writePos = (_head + _count) % _maxRetainedCandles;
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
