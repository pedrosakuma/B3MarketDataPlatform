namespace B3.Umdf.Book;

/// <summary>
/// Per-security trade-derived state owned by <see cref="BookManager"/>:
/// the recent-trades ring used for snapshot history, a 1-second OHLCV
/// candle aggregator, and a small (id → record) index that lets the
/// <c>TradeBust_57</c> reversal path locate the original price/qty/timestamp
/// of a busted trade and recompute the affected bucket.
/// </summary>
/// <remarks>
/// All mutations happen on the owning BookManager's single feed thread —
/// no locks needed. Snapshot reads (e.g., <see cref="CandleAggregator.GetCandles"/>)
/// remain thread-safe via the aggregator's version-stamp spin-copy.
/// </remarks>
internal sealed class SymbolTradeState
{
    internal readonly struct TradeRecord
    {
        public readonly long Price;
        public readonly long Qty;
        public readonly long TimestampSeconds;
        public TradeRecord(long price, long qty, long ts)
        { Price = price; Qty = qty; TimestampSeconds = ts; }
    }

    public TradeRingBuffer Ring { get; }
    public CandleAggregator Candles { get; } = new();

    private readonly Dictionary<long, TradeRecord> _byId;
    private readonly Queue<long> _idOrder;
    private readonly int _capacity;

    /// <summary>Last applied (non-busted) trade price, or null if no trade yet.</summary>
    public long? LastTradePrice { get; private set; }
    /// <summary>Last applied (non-busted) trade quantity, or null if no trade yet.</summary>
    public long? LastTradeQuantity { get; private set; }

    public SymbolTradeState(int capacity)
    {
        _capacity = capacity;
        Ring = new TradeRingBuffer(capacity);
        _byId = new Dictionary<long, TradeRecord>(capacity);
        _idOrder = new Queue<long>(capacity);
    }

    public void AddTrade(long price, long qty, long tradeId, long timestampSeconds)
    {
        Ring.Add(price, qty, tradeId);
        Candles.Add(price, qty, timestampSeconds);
        LastTradePrice = price;
        LastTradeQuantity = qty;

        if (_byId.ContainsKey(tradeId))
        {
            _byId[tradeId] = new TradeRecord(price, qty, timestampSeconds);
            return;
        }

        while (_byId.Count >= _capacity && _idOrder.Count > 0)
        {
            var oldest = _idOrder.Dequeue();
            _byId.Remove(oldest);
        }
        _byId[tradeId] = new TradeRecord(price, qty, timestampSeconds);
        _idOrder.Enqueue(tradeId);
    }

    public bool TryGetTrade(long tradeId, out TradeRecord rec)
        => _byId.TryGetValue(tradeId, out rec);

    public void RemoveTradeFromIndex(long tradeId) => _byId.Remove(tradeId);

    /// <summary>
    /// Enumerate the remaining tracked trades that fall in the given
    /// 1-second bucket. Used by the bust-reversal path to recompute OHLCV.
    /// </summary>
    public List<TradeRecord> GetTradesInBucket(long timestampSeconds)
    {
        var list = new List<TradeRecord>();
        foreach (var kv in _byId)
            if (kv.Value.TimestampSeconds == timestampSeconds)
                list.Add(kv.Value);
        return list;
    }

    /// <summary>
    /// After a bust mutates the ring, refresh <see cref="LastTradePrice"/>
    /// from the most recent non-busted slot. Falls back to the previous
    /// candle's close if every retained ring slot is now busted; nulls
    /// out if neither is available.
    /// </summary>
    public void RefreshLastTradeFromRing()
    {
        if (Ring.TryGetLastNonBustedPrice(out var price, out var qty, out _))
        {
            LastTradePrice = price;
            LastTradeQuantity = qty;
            return;
        }
        var fallback = Candles.LastClose;
        if (fallback != 0)
        {
            LastTradePrice = fallback;
            LastTradeQuantity = 0;
        }
        else
        {
            LastTradePrice = null;
            LastTradeQuantity = null;
        }
    }
}
